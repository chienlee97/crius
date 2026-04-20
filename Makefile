# Makefile for crius vendor patch workflow

.PHONY: all build clean check-patch apply-patch run test

# 默认目标
all: build

# 检查补丁是否已应用
check-patch:
	@echo "检查vendor补丁状态..."
	@if grep -q "is_connect" vendor/h2/src/server.rs && \
		grep -q "authority.*not required.*HTTP/2" vendor/h2/src/server.rs && \
		grep -q "cfg(all(any(target_os = \"linux\", target_os = \"android\"), feature = \"tokio-vsock\"))" vendor/ttrpc/src/asynchronous/server.rs && \
		grep -q "cfg(all(any(target_os = \"linux\", target_os = \"android\"), feature = \"tokio-vsock\"))" vendor/ttrpc/src/asynchronous/transport/mod.rs && \
		! grep -q "\"tokio-vsock\"," vendor/ttrpc/Cargo.toml; then \
		echo "vendor补丁已应用"; \
		exit 0; \
	else \
		echo "vendor补丁未应用"; \
		exit 1; \
	fi

# 应用补丁
apply-patch:
	@cargo vendor
	@echo "应用h2 authority补丁..."
	@echo "应用PR#612修改到vendor/h2/src/server.rs..."
	@sed -i '1557,1568c\
        // A request translated from HTTP/1 must not include the :authority\
        // header\
        if let Some(authority) = pseudo.authority {\
            // `:authority` is required only with `CONNECT` method.\
            // It should contains host and port. This is exactly what `uri::Authority` is\
            // going to parse.\
            //\
            // See: https://datatracker.ietf.org/doc/html/rfc7540#section-8.3\
            let maybe_authority = uri::Authority::from_maybe_shared(authority.clone().into_inner());\
            if is_connect {\
                if let Err(why) = &maybe_authority {\
                    malformed!(\
                        "malformed headers: malformed authority ({:?}): {}",\
                        authority,\
                        why,\
                    );\
                }\
            }\
\
            // `authority` is not required in HTTP/2, so it is safe to keep it `None`\
            // in `parts`.\
            //\
            // See: https://datatracker.ietf.org/doc/html/rfc7540#section-8.1.2.3\
            parts.authority = maybe_authority.ok();\
        }' vendor/h2/src/server.rs && \
	echo "更新checksum..." && \
	NEW_CHECKSUM=$$(sha256sum vendor/h2/src/server.rs | cut -d' ' -f1) && \
	sed -i "s/\"src\/server.rs\":\"[^\"]*\"/\"src\/server.rs\":\"$$NEW_CHECKSUM\"/" vendor/h2/.cargo-checksum.json && \
	echo "h2补丁应用成功"
	@echo "应用ttrpc async补丁..."
	@sed -i '/"tokio-vsock",/d' vendor/ttrpc/Cargo.toml && \
	sed -i 's/#\[cfg(any(target_os = "linux", target_os = "android"))\]/#[cfg(all(any(target_os = "linux", target_os = "android"), feature = "tokio-vsock"))]/g' vendor/ttrpc/src/asynchronous/server.rs && \
	sed -i 's/#\[cfg(any(target_os = "linux", target_os = "android"))\]/#[cfg(all(any(target_os = "linux", target_os = "android"), feature = "tokio-vsock"))]/g' vendor/ttrpc/src/asynchronous/transport/mod.rs && \
	TTRPC_TOML_SUM=$$(sha256sum vendor/ttrpc/Cargo.toml | cut -d' ' -f1) && \
	TTRPC_SERVER_SUM=$$(sha256sum vendor/ttrpc/src/asynchronous/server.rs | cut -d' ' -f1) && \
	TTRPC_TRANSPORT_SUM=$$(sha256sum vendor/ttrpc/src/asynchronous/transport/mod.rs | cut -d' ' -f1) && \
	sed -i "s/\"Cargo.toml\":\"[^\"]*\"/\"Cargo.toml\":\"$$TTRPC_TOML_SUM\"/" vendor/ttrpc/.cargo-checksum.json && \
	sed -i "s/\"src\/asynchronous\/server.rs\":\"[^\"]*\"/\"src\/asynchronous\/server.rs\":\"$$TTRPC_SERVER_SUM\"/" vendor/ttrpc/.cargo-checksum.json && \
	sed -i "s/\"src\/asynchronous\/transport\/mod.rs\":\"[^\"]*\"/\"src\/asynchronous\/transport\/mod.rs\":\"$$TTRPC_TRANSPORT_SUM\"/" vendor/ttrpc/.cargo-checksum.json && \
	echo "ttrpc补丁应用成功"
	@echo "vendor补丁应用成功"

# 构建项目（自动检查和应用补丁）
build: 
	@echo "开始构建crius..."
	@$(MAKE) check-patch || $(MAKE) apply-patch
	cargo build
	@echo "构建完成"

# 运行服务
run: build
	@echo "启动crius服务..."
	cargo run --bin crius -- --listen unix:///tmp/crius.sock

# 测试crictl兼容性
test: build
	@echo "测试crictl兼容性..."
	@cargo run --bin crius -- --listen unix:///tmp/crius.sock & \
		SERVER_PID=$$!; \
		sleep 3; \
		if CONTAINER_RUNTIME_ENDPOINT=unix:///tmp/crius.sock crictl version; then \
			echo "crictl兼容性测试通过"; \
		else \
			echo "crictl兼容性测试失败"; \
		fi; \
		kill $$SERVER_PID 2>/dev/null || true

# 清理
clean:
	@echo "清理构建产物..."
	cargo clean

# 重新构建（强制重新应用补丁）
rebuild: clean build

# 显示帮助
help:
	@echo "可用命令:"
	@echo "  all        - 构建项目（默认）"
	@echo "  build      - 构建项目（自动检查和应用补丁）"
	@echo "  check-patch - 检查补丁是否已应用"
	@echo "  apply-patch - 应用vendor补丁"
	@echo "  run        - 构建并运行服务"
	@echo "  test       - 构建并测试crictl兼容性"
	@echo "  clean      - 清理构建产物"
	@echo "  rebuild    - 清理并重新构建"
	@echo "  help       - 显示此帮助信息"
