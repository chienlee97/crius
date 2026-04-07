#[path = "spdy_dictionary.rs"]
mod spdy_dictionary;

use std::ffi::{c_char, CStr};
use std::io::{self, Read, Write};
use std::mem;
use std::os::raw::{c_int, c_uint, c_ulong, c_void};
use std::ptr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use spdy_dictionary::HEADER_DICTIONARY;

pub const SPDY_31: &str = "SPDY/3.1";
pub const STREAM_PROTOCOL_V2: &str = "v2.channel.k8s.io";

pub type StreamId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFrameType {
    SynStream = 0x0001,
    SynReply = 0x0002,
    RstStream = 0x0003,
    Settings = 0x0004,
    Ping = 0x0006,
    GoAway = 0x0007,
    Headers = 0x0008,
    WindowUpdate = 0x0009,
}

impl ControlFrameType {
    fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x0001 => Some(Self::SynStream),
            0x0002 => Some(Self::SynReply),
            0x0003 => Some(Self::RstStream),
            0x0004 => Some(Self::Settings),
            0x0006 => Some(Self::Ping),
            0x0007 => Some(Self::GoAway),
            0x0008 => Some(Self::Headers),
            0x0009 => Some(Self::WindowUpdate),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlFrameHeader {
    pub version: u16,
    pub frame_type: u16,
    pub flags: u8,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynStreamFrame {
    pub flags: u8,
    pub stream_id: StreamId,
    pub associated_to_stream_id: StreamId,
    pub priority: u8,
    pub slot: u8,
    pub header_block: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynReplyFrame {
    pub flags: u8,
    pub stream_id: StreamId,
    pub header_block: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFrame {
    pub stream_id: StreamId,
    pub flags: u8,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingFrame {
    pub id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoAwayFrame {
    pub last_good_stream_id: StreamId,
    pub status: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowUpdateFrame {
    pub stream_id: StreamId,
    pub delta_window_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SynStream(SynStreamFrame),
    SynReply(SynReplyFrame),
    Data(DataFrame),
    Ping(PingFrame),
    GoAway(GoAwayFrame),
    WindowUpdate(WindowUpdateFrame),
    UnsupportedControl(ControlFrameHeader, Vec<u8>),
}

const Z_OK: c_int = 0;
const Z_STREAM_END: c_int = 1;
const Z_NEED_DICT: c_int = 2;
const Z_BUF_ERROR: c_int = -5;
const Z_SYNC_FLUSH: c_int = 2;
const Z_BEST_COMPRESSION: c_int = 9;

type AllocFunc = Option<unsafe extern "C" fn(*mut c_void, c_uint, c_uint) -> *mut c_void>;
type FreeFunc = Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>;

#[repr(C)]
struct ZStream {
    next_in: *const u8,
    avail_in: c_uint,
    total_in: c_ulong,
    next_out: *mut u8,
    avail_out: c_uint,
    total_out: c_ulong,
    msg: *const c_char,
    state: *mut c_void,
    zalloc: AllocFunc,
    zfree: FreeFunc,
    opaque: *mut c_void,
    data_type: c_int,
    adler: c_ulong,
    reserved: c_ulong,
}

#[link(name = "z")]
unsafe extern "C" {
    fn zlibVersion() -> *const c_char;
    fn deflateInit_(
        strm: *mut ZStream,
        level: c_int,
        version: *const c_char,
        stream_size: c_int,
    ) -> c_int;
    fn deflateSetDictionary(
        strm: *mut ZStream,
        dictionary: *const u8,
        dict_length: c_uint,
    ) -> c_int;
    fn deflate(strm: *mut ZStream, flush: c_int) -> c_int;
    fn deflateEnd(strm: *mut ZStream) -> c_int;

    fn inflateInit_(strm: *mut ZStream, version: *const c_char, stream_size: c_int) -> c_int;
    fn inflateSetDictionary(
        strm: *mut ZStream,
        dictionary: *const u8,
        dict_length: c_uint,
    ) -> c_int;
    fn inflate(strm: *mut ZStream, flush: c_int) -> c_int;
    fn inflateEnd(strm: *mut ZStream) -> c_int;
}

fn zlib_error(context: &str, code: c_int, stream: &ZStream) -> io::Error {
    let detail = if stream.msg.is_null() {
        format!("zlib error code {}", code)
    } else {
        unsafe { CStr::from_ptr(stream.msg) }
            .to_string_lossy()
            .into_owned()
    };
    io::Error::new(io::ErrorKind::Other, format!("{}: {}", context, detail))
}

pub struct HeaderCompressor {
    stream: Box<ZStream>,
    initialized: bool,
}

impl HeaderCompressor {
    pub fn new() -> Self {
        Self::try_new().expect("failed to initialize SPDY header compressor")
    }

    fn try_new() -> io::Result<Self> {
        let mut stream = Box::new(unsafe { mem::zeroed::<ZStream>() });
        let version = unsafe { zlibVersion() };
        let init = unsafe {
            deflateInit_(
                stream.as_mut(),
                Z_BEST_COMPRESSION,
                version,
                mem::size_of::<ZStream>() as c_int,
            )
        };
        if init != Z_OK {
            return Err(zlib_error("deflateInit_", init, &stream));
        }
        let dict = unsafe {
            deflateSetDictionary(
                stream.as_mut(),
                HEADER_DICTIONARY.as_ptr(),
                HEADER_DICTIONARY.len() as c_uint,
            )
        };
        if dict != Z_OK {
            unsafe {
                let _ = deflateEnd(stream.as_mut());
            }
            return Err(zlib_error("deflateSetDictionary", dict, &stream));
        }

        Ok(Self {
            stream,
            initialized: true,
        })
    }

    pub fn compress(&mut self, block: &[u8]) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        let stream = self.stream.as_mut();
        stream.next_in = if block.is_empty() {
            ptr::null()
        } else {
            block.as_ptr()
        };
        stream.avail_in = block.len() as c_uint;

        loop {
            let mut chunk = [0u8; 4096];
            stream.next_out = chunk.as_mut_ptr();
            stream.avail_out = chunk.len() as c_uint;
            let status = unsafe { deflate(stream, Z_SYNC_FLUSH) };
            if status != Z_OK {
                return Err(zlib_error("deflate", status, stream));
            }

            let written = chunk.len() - stream.avail_out as usize;
            output.extend_from_slice(&chunk[..written]);

            if stream.avail_in == 0 && stream.avail_out != 0 {
                break;
            }
        }

        Ok(output)
    }
}

impl Drop for HeaderCompressor {
    fn drop(&mut self) {
        if self.initialized {
            unsafe {
                let _ = deflateEnd(self.stream.as_mut());
            }
        }
    }
}

unsafe impl Send for HeaderCompressor {}

pub struct HeaderDecompressor {
    stream: Box<ZStream>,
    initialized: bool,
    dictionary_set: bool,
}

impl HeaderDecompressor {
    pub fn new() -> Self {
        Self::try_new().expect("failed to initialize SPDY header decompressor")
    }

    fn try_new() -> io::Result<Self> {
        let mut stream = Box::new(unsafe { mem::zeroed::<ZStream>() });
        let version = unsafe { zlibVersion() };
        let init =
            unsafe { inflateInit_(stream.as_mut(), version, mem::size_of::<ZStream>() as c_int) };
        if init != Z_OK {
            return Err(zlib_error("inflateInit_", init, &stream));
        }

        Ok(Self {
            stream,
            initialized: true,
            dictionary_set: false,
        })
    }

    pub fn decompress(&mut self, block: &[u8]) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        let stream = self.stream.as_mut();
        stream.next_in = if block.is_empty() {
            ptr::null()
        } else {
            block.as_ptr()
        };
        stream.avail_in = block.len() as c_uint;

        loop {
            let mut chunk = [0u8; 4096];
            stream.next_out = chunk.as_mut_ptr();
            stream.avail_out = chunk.len() as c_uint;

            let status = unsafe { inflate(stream, Z_SYNC_FLUSH) };
            let written = chunk.len() - stream.avail_out as usize;
            output.extend_from_slice(&chunk[..written]);

            match status {
                Z_OK | Z_STREAM_END => {
                    if stream.avail_in == 0 && stream.avail_out != 0 {
                        break;
                    }
                }
                Z_NEED_DICT => {
                    if self.dictionary_set {
                        return Err(zlib_error(
                            "inflate requested dictionary twice",
                            status,
                            &self.stream,
                        ));
                    }
                    let dict = unsafe {
                        inflateSetDictionary(
                            stream,
                            HEADER_DICTIONARY.as_ptr(),
                            HEADER_DICTIONARY.len() as c_uint,
                        )
                    };
                    if dict != Z_OK {
                        return Err(zlib_error("inflateSetDictionary", dict, stream));
                    }
                    self.dictionary_set = true;
                }
                Z_BUF_ERROR => {
                    if stream.avail_in == 0 && stream.avail_out != 0 {
                        break;
                    }
                }
                _ => return Err(zlib_error("inflate", status, stream)),
            }
        }

        Ok(output)
    }
}

impl Drop for HeaderDecompressor {
    fn drop(&mut self) {
        if self.initialized {
            unsafe {
                let _ = inflateEnd(self.stream.as_mut());
            }
        }
    }
}

unsafe impl Send for HeaderDecompressor {}

pub struct SpdyWriter<W: Write> {
    writer: W,
    header_compressor: HeaderCompressor,
}

pub struct AsyncSpdyWriter<W: AsyncWrite + Unpin> {
    writer: W,
    header_compressor: HeaderCompressor,
}

impl<W: AsyncWrite + Unpin> AsyncSpdyWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_compressor: HeaderCompressor::new(),
        }
    }

    pub async fn write_syn_reply(
        &mut self,
        stream_id: StreamId,
        headers: &[(String, String)],
        fin: bool,
    ) -> io::Result<()> {
        let header_block = encode_header_block(headers);
        let compressed = self.header_compressor.compress(&header_block)?;
        let flags = if fin { 0x01 } else { 0x00 };
        let length = 4 + compressed.len() as u32;
        write_control_frame_header_async(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::SynReply as u16,
                flags,
                length,
            },
        )
        .await?;
        write_u32_async(&mut self.writer, stream_id).await?;
        self.writer.write_all(&compressed).await?;
        self.writer.flush().await
    }

    pub async fn write_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        fin: bool,
    ) -> io::Result<()> {
        let stream_id = stream_id & 0x7fff_ffff;
        write_u32_async(&mut self.writer, stream_id).await?;
        let flags = if fin { 0x01u32 } else { 0u32 };
        let length = data.len() as u32 & 0x00ff_ffff;
        write_u32_async(&mut self.writer, (flags << 24) | length).await?;
        self.writer.write_all(data).await?;
        self.writer.flush().await
    }

    pub async fn write_ping(&mut self, id: u32) -> io::Result<()> {
        write_control_frame_header_async(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::Ping as u16,
                flags: 0,
                length: 4,
            },
        )
        .await?;
        write_u32_async(&mut self.writer, id).await?;
        self.writer.flush().await
    }

    pub async fn write_goaway(&mut self, last_good_stream_id: StreamId) -> io::Result<()> {
        write_control_frame_header_async(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::GoAway as u16,
                flags: 0,
                length: 8,
            },
        )
        .await?;
        write_u32_async(&mut self.writer, last_good_stream_id).await?;
        write_u32_async(&mut self.writer, 0).await?;
        self.writer.flush().await
    }
}

impl<W: Write> SpdyWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_compressor: HeaderCompressor::new(),
        }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn write_syn_reply(
        &mut self,
        stream_id: StreamId,
        headers: &[(String, String)],
        fin: bool,
    ) -> io::Result<()> {
        let header_block = encode_header_block(headers);
        let compressed = self.header_compressor.compress(&header_block)?;
        let flags = if fin { 0x01 } else { 0x00 };
        let length = 4 + compressed.len() as u32;
        write_control_frame_header(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::SynReply as u16,
                flags,
                length,
            },
        )?;
        write_u32(&mut self.writer, stream_id)?;
        self.writer.write_all(&compressed)?;
        self.writer.flush()
    }

    pub fn write_data(&mut self, stream_id: StreamId, data: &[u8], fin: bool) -> io::Result<()> {
        let stream_id = stream_id & 0x7fff_ffff;
        write_u32(&mut self.writer, stream_id)?;
        let flags = if fin { 0x01u32 } else { 0u32 };
        let length = data.len() as u32 & 0x00ff_ffff;
        write_u32(&mut self.writer, (flags << 24) | length)?;
        self.writer.write_all(data)?;
        self.writer.flush()
    }

    pub fn write_ping(&mut self, id: u32) -> io::Result<()> {
        write_control_frame_header(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::Ping as u16,
                flags: 0,
                length: 4,
            },
        )?;
        write_u32(&mut self.writer, id)?;
        self.writer.flush()
    }

    pub fn write_goaway(&mut self, last_good_stream_id: StreamId) -> io::Result<()> {
        write_control_frame_header(
            &mut self.writer,
            ControlFrameHeader {
                version: 3,
                frame_type: ControlFrameType::GoAway as u16,
                flags: 0,
                length: 8,
            },
        )?;
        write_u32(&mut self.writer, last_good_stream_id)?;
        write_u32(&mut self.writer, 0)?;
        self.writer.flush()
    }
}

pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Frame> {
    let first_word = read_u32(reader)?;
    if first_word & 0x8000_0000 != 0 {
        let version = ((first_word >> 16) & 0x7fff) as u16;
        let frame_type_raw = (first_word & 0xffff) as u16;
        let second_word = read_u32(reader)?;
        let header = ControlFrameHeader {
            version,
            frame_type: frame_type_raw,
            flags: (second_word >> 24) as u8,
            length: second_word & 0x00ff_ffff,
        };
        return read_control_frame(reader, header);
    }

    let stream_id = first_word & 0x7fff_ffff;
    let second_word = read_u32(reader)?;
    let flags = (second_word >> 24) as u8;
    let length = (second_word & 0x00ff_ffff) as usize;
    let mut data = vec![0u8; length];
    reader.read_exact(&mut data)?;
    Ok(Frame::Data(DataFrame {
        stream_id,
        flags,
        data,
    }))
}

pub async fn read_frame_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
    let first_word = read_u32_async(reader).await?;
    if first_word & 0x8000_0000 != 0 {
        let version = ((first_word >> 16) & 0x7fff) as u16;
        let frame_type_raw = (first_word & 0xffff) as u16;
        let second_word = read_u32_async(reader).await?;
        let header = ControlFrameHeader {
            version,
            frame_type: frame_type_raw,
            flags: (second_word >> 24) as u8,
            length: second_word & 0x00ff_ffff,
        };
        return read_control_frame_async(reader, header).await;
    }

    let stream_id = first_word & 0x7fff_ffff;
    let second_word = read_u32_async(reader).await?;
    let flags = (second_word >> 24) as u8;
    let length = (second_word & 0x00ff_ffff) as usize;
    let mut data = vec![0u8; length];
    reader.read_exact(&mut data).await?;
    Ok(Frame::Data(DataFrame {
        stream_id,
        flags,
        data,
    }))
}

fn read_control_frame<R: Read>(reader: &mut R, header: ControlFrameHeader) -> io::Result<Frame> {
    match ControlFrameType::from_u16(header.frame_type) {
        Some(ControlFrameType::SynStream) => {
            let stream_id = read_u32(reader)? & 0x7fff_ffff;
            let associated_to_stream_id = read_u32(reader)? & 0x7fff_ffff;
            let priority = read_u8(reader)? >> 5;
            let slot = read_u8(reader)?;
            let remaining = header.length.saturating_sub(10) as usize;
            let mut header_block = vec![0u8; remaining];
            reader.read_exact(&mut header_block)?;
            Ok(Frame::SynStream(SynStreamFrame {
                flags: header.flags,
                stream_id,
                associated_to_stream_id,
                priority,
                slot,
                header_block,
            }))
        }
        Some(ControlFrameType::SynReply) => {
            let stream_id = read_u32(reader)? & 0x7fff_ffff;
            let remaining = header.length.saturating_sub(4) as usize;
            let mut header_block = vec![0u8; remaining];
            reader.read_exact(&mut header_block)?;
            Ok(Frame::SynReply(SynReplyFrame {
                flags: header.flags,
                stream_id,
                header_block,
            }))
        }
        Some(ControlFrameType::Ping) => Ok(Frame::Ping(PingFrame {
            id: read_u32(reader)?,
        })),
        Some(ControlFrameType::GoAway) => Ok(Frame::GoAway(GoAwayFrame {
            last_good_stream_id: read_u32(reader)? & 0x7fff_ffff,
            status: read_u32(reader)?,
        })),
        Some(ControlFrameType::WindowUpdate) => Ok(Frame::WindowUpdate(WindowUpdateFrame {
            stream_id: read_u32(reader)? & 0x7fff_ffff,
            delta_window_size: read_u32(reader)?,
        })),
        _ => {
            let mut payload = vec![0u8; header.length as usize];
            reader.read_exact(&mut payload)?;
            Ok(Frame::UnsupportedControl(header, payload))
        }
    }
}

async fn read_control_frame_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    header: ControlFrameHeader,
) -> io::Result<Frame> {
    match ControlFrameType::from_u16(header.frame_type) {
        Some(ControlFrameType::SynStream) => {
            let stream_id = read_u32_async(reader).await? & 0x7fff_ffff;
            let associated_to_stream_id = read_u32_async(reader).await? & 0x7fff_ffff;
            let priority = read_u8_async(reader).await? >> 5;
            let slot = read_u8_async(reader).await?;
            let remaining = header.length.saturating_sub(10) as usize;
            let mut header_block = vec![0u8; remaining];
            reader.read_exact(&mut header_block).await?;
            Ok(Frame::SynStream(SynStreamFrame {
                flags: header.flags,
                stream_id,
                associated_to_stream_id,
                priority,
                slot,
                header_block,
            }))
        }
        Some(ControlFrameType::SynReply) => {
            let stream_id = read_u32_async(reader).await? & 0x7fff_ffff;
            let remaining = header.length.saturating_sub(4) as usize;
            let mut header_block = vec![0u8; remaining];
            reader.read_exact(&mut header_block).await?;
            Ok(Frame::SynReply(SynReplyFrame {
                flags: header.flags,
                stream_id,
                header_block,
            }))
        }
        Some(ControlFrameType::Ping) => Ok(Frame::Ping(PingFrame {
            id: read_u32_async(reader).await?,
        })),
        Some(ControlFrameType::GoAway) => Ok(Frame::GoAway(GoAwayFrame {
            last_good_stream_id: read_u32_async(reader).await? & 0x7fff_ffff,
            status: read_u32_async(reader).await?,
        })),
        Some(ControlFrameType::WindowUpdate) => Ok(Frame::WindowUpdate(WindowUpdateFrame {
            stream_id: read_u32_async(reader).await? & 0x7fff_ffff,
            delta_window_size: read_u32_async(reader).await?,
        })),
        _ => {
            let mut payload = vec![0u8; header.length as usize];
            reader.read_exact(&mut payload).await?;
            Ok(Frame::UnsupportedControl(header, payload))
        }
    }
}

pub fn encode_header_block(headers: &[(String, String)]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32_to_vec(&mut buf, headers.len() as u32);
    for (name, value) in headers {
        let lower_name = name.to_ascii_lowercase();
        write_u32_to_vec(&mut buf, lower_name.len() as u32);
        buf.extend_from_slice(lower_name.as_bytes());
        write_u32_to_vec(&mut buf, value.len() as u32);
        buf.extend_from_slice(value.as_bytes());
    }
    buf
}

pub fn decode_header_block(
    compressed_block: &[u8],
    decompressor: &mut HeaderDecompressor,
) -> io::Result<Vec<(String, String)>> {
    let block = decompressor.decompress(compressed_block)?;
    let mut offset = 0usize;
    if block.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "missing SPDY header count",
        ));
    }

    let header_count = u32::from_be_bytes(block[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        if offset + 4 > block.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing SPDY header name length",
            ));
        }
        let name_len = u32::from_be_bytes(block[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + name_len > block.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated SPDY header name",
            ));
        }
        let name = String::from_utf8(block[offset..offset + name_len].to_vec()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid SPDY header name: {}", e),
            )
        })?;
        offset += name_len;

        if offset + 4 > block.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing SPDY header value length",
            ));
        }
        let value_len = u32::from_be_bytes(block[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + value_len > block.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated SPDY header value",
            ));
        }
        let value = String::from_utf8(block[offset..offset + value_len].to_vec()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid SPDY header value: {}", e),
            )
        })?;
        offset += value_len;
        headers.push((name, value));
    }

    Ok(headers)
}

pub fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .and_then(|(_, value)| value.split('\0').next())
}

fn write_control_frame_header<W: Write>(
    writer: &mut W,
    header: ControlFrameHeader,
) -> io::Result<()> {
    let first_word =
        0x8000_0000u32 | (((header.version as u32) & 0x7fff) << 16) | (header.frame_type as u32);
    let second_word = ((header.flags as u32) << 24) | (header.length & 0x00ff_ffff);
    write_u32(writer, first_word)?;
    write_u32(writer, second_word)
}

async fn write_control_frame_header_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: ControlFrameHeader,
) -> io::Result<()> {
    let first_word =
        0x8000_0000u32 | (((header.version as u32) & 0x7fff) << 16) | (header.frame_type as u32);
    let second_word = ((header.flags as u32) << 24) | (header.length & 0x00ff_ffff);
    write_u32_async(writer, first_word).await?;
    write_u32_async(writer, second_word).await
}

fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

async fn read_u8_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).await?;
    Ok(buf[0])
}

fn read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

async fn read_u32_async<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

async fn write_u32_async<W: AsyncWrite + Unpin>(writer: &mut W, value: u32) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes()).await
}

fn write_u32_to_vec(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_header_block() {
        let encoded = encode_header_block(&[
            ("streamtype".to_string(), "stdout".to_string()),
            ("x-test".to_string(), "1".to_string()),
        ]);
        assert_eq!(&encoded[..4], 2u32.to_be_bytes().as_slice());
    }

    #[test]
    fn test_read_data_frame() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1u32).to_be_bytes());
        bytes.extend_from_slice(&(3u32).to_be_bytes());
        bytes.extend_from_slice(b"abc");

        let frame = read_frame(&mut &bytes[..]).unwrap();
        assert_eq!(
            frame,
            Frame::Data(DataFrame {
                stream_id: 1,
                flags: 0,
                data: b"abc".to_vec(),
            })
        );
    }

    #[test]
    fn test_write_syn_reply() {
        let mut writer = SpdyWriter::new(Vec::<u8>::new());
        writer
            .write_syn_reply(
                1,
                &[("streamtype".to_string(), "stdout".to_string())],
                false,
            )
            .unwrap();
        let buf = writer.into_inner();
        assert!(!buf.is_empty());
        assert_eq!(buf[0] & 0x80, 0x80);
    }

    #[test]
    fn test_header_block_roundtrip_with_dictionary_state() {
        let mut compressor = HeaderCompressor::new();
        let mut decompressor = HeaderDecompressor::new();

        let first = compressor
            .compress(&encode_header_block(&[
                ("streamtype".to_string(), "error".to_string()),
                ("x-test".to_string(), "1".to_string()),
            ]))
            .unwrap();
        let second = compressor
            .compress(&encode_header_block(&[(
                "streamtype".to_string(),
                "stdout".to_string(),
            )]))
            .unwrap();

        let first_headers = decode_header_block(&first, &mut decompressor).unwrap();
        let second_headers = decode_header_block(&second, &mut decompressor).unwrap();

        assert_eq!(header_value(&first_headers, "streamtype"), Some("error"));
        assert_eq!(header_value(&second_headers, "streamtype"), Some("stdout"));
    }
}
