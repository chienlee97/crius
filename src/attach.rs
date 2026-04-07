use std::io;

pub const ATTACH_PIPE_STDIN: u8 = 1;
pub const ATTACH_PIPE_STDOUT: u8 = 2;
pub const ATTACH_PIPE_STDERR: u8 = 3;

const ATTACH_FRAME_HEADER_LEN: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachOutputFrame {
    pub pipe: u8,
    pub payload: Vec<u8>,
}

pub fn encode_attach_output_frame(pipe: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(ATTACH_FRAME_HEADER_LEN + payload.len());
    frame.push(pipe);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

#[derive(Debug, Default)]
pub struct AttachOutputDecoder {
    buffer: Vec<u8>,
}

impl AttachOutputDecoder {
    pub fn push(&mut self, chunk: &[u8]) -> io::Result<Vec<AttachOutputFrame>> {
        self.buffer.extend_from_slice(chunk);
        let mut offset = 0usize;
        let mut frames = Vec::new();

        loop {
            if self.buffer.len().saturating_sub(offset) < ATTACH_FRAME_HEADER_LEN {
                break;
            }

            let pipe = self.buffer[offset];
            let length = u32::from_be_bytes([
                self.buffer[offset + 1],
                self.buffer[offset + 2],
                self.buffer[offset + 3],
                self.buffer[offset + 4],
            ]) as usize;
            let frame_end = offset + ATTACH_FRAME_HEADER_LEN + length;
            if frame_end > self.buffer.len() {
                break;
            }

            if pipe != ATTACH_PIPE_STDOUT && pipe != ATTACH_PIPE_STDERR {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown attach output pipe id {}", pipe),
                ));
            }

            frames.push(AttachOutputFrame {
                pipe,
                payload: self.buffer[offset + ATTACH_FRAME_HEADER_LEN..frame_end].to_vec(),
            });
            offset = frame_end;
        }

        if offset > 0 {
            self.buffer.drain(..offset);
        }

        Ok(frames)
    }

    pub fn finish(&self) -> io::Result<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete attach output frame",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_and_decode_attach_output_frames() {
        let stdout = encode_attach_output_frame(ATTACH_PIPE_STDOUT, b"hello");
        let stderr = encode_attach_output_frame(ATTACH_PIPE_STDERR, b"oops");

        let mut decoder = AttachOutputDecoder::default();
        let mut frames = decoder.push(&stdout[..3]).unwrap();
        assert!(frames.is_empty());

        frames.extend(decoder.push(&stdout[3..]).unwrap());
        frames.extend(decoder.push(&stderr).unwrap());

        assert_eq!(
            frames,
            vec![
                AttachOutputFrame {
                    pipe: ATTACH_PIPE_STDOUT,
                    payload: b"hello".to_vec(),
                },
                AttachOutputFrame {
                    pipe: ATTACH_PIPE_STDERR,
                    payload: b"oops".to_vec(),
                },
            ]
        );
        decoder.finish().unwrap();
    }
}
