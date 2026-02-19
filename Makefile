.PHONY: build build-wasm ui dev-ui deploy clean

# Build the native graphdb binary.
build:
	go build -o bin/graphdb ./cmd/graphdb

# Build the WASM binary and copy Go runtime to ui/public/.
build-wasm:
	@mkdir -p ui/public
	GOOS=js GOARCH=wasm go build -o ui/public/graphdb.wasm ./cmd/graphdb-wasm
	@cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" ui/public/wasm_exec.js 2>/dev/null || \
	 cp "$$(go env GOROOT)/misc/wasm/wasm_exec.js" ui/public/wasm_exec.js
	@echo "WASM build complete: ui/public/graphdb.wasm ($$(du -h ui/public/graphdb.wasm | cut -f1))"

# Build the UI for production (includes WASM assets from ui/public/).
ui: build-wasm
	cd ui && npm install && npm run build

# Start the UI dev server in WASM demo mode.
dev-ui: build-wasm
	cd ui && VITE_WASM=true npm run dev

# Deploy to Cloudflare Workers (requires: npm i -g wrangler && wrangler login)
deploy: ui
	cd ui && npx wrangler deploy

clean:
	rm -rf bin/ ui/dist/ ui/public/graphdb.wasm ui/public/wasm_exec.js
