# cbinding Makefile — build libbufarrow shared library
.POSIX:

MODULE_ROOT := ..
EXT          = so
LIB          = libbufarrow.$(EXT)

# macOS detection
ifeq ($(shell uname -s),Darwin)
  EXT = dylib
  LIB = libbufarrow.$(EXT)
endif

GO       ?= go
CC       ?= cc
GOFLAGS  ?=

.PHONY: build clean test build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-all

build: $(LIB)

$(LIB):
	cd $(MODULE_ROOT) && \
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared \
		-tags cgo \
		-o cbinding/$(LIB) \
		$(GOFLAGS) \
		./cbinding

build-linux-amd64:
	cd $(MODULE_ROOT) && \
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 CC=$${CC_LINUX_AMD64:-gcc} \
	$(GO) build -buildmode=c-shared -tags cgo -o cbinding/libbufarrow-linux-amd64.so ./cbinding

build-linux-arm64:
	cd $(MODULE_ROOT) && \
	GOOS=linux GOARCH=arm64 CGO_ENABLED=1 CC=$${CC_LINUX_ARM64:-aarch64-linux-gnu-gcc} \
	$(GO) build -buildmode=c-shared -tags cgo -o cbinding/libbufarrow-linux-arm64.so ./cbinding

build-darwin-amd64:
	cd $(MODULE_ROOT) && \
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 \
	$(GO) build -buildmode=c-shared -tags cgo -o cbinding/libbufarrow-darwin-amd64.dylib ./cbinding

build-darwin-arm64:
	cd $(MODULE_ROOT) && \
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=1 \
	$(GO) build -buildmode=c-shared -tags cgo -o cbinding/libbufarrow-darwin-arm64.dylib ./cbinding

build-all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64

test:
	cd $(MODULE_ROOT) && $(GO) test -tags cgo -v ./cbinding/...

clean:
	rm -f libbufarrow.so libbufarrow.dylib libbufarrow-*.so libbufarrow-*.dylib libbufarrow.h
