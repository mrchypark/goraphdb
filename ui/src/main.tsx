import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { isWasmMode, initWasmDemo } from './api/client'
import './index.css'

const demoMode = isWasmMode()

async function boot() {
  if (demoMode) {
    console.log('[graphdb] WASM demo mode — loading database into browser…')
    await initWasmDemo()
    console.log('[graphdb] WASM database ready with demo data.')
  }

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <BrowserRouter basename={demoMode ? '/demo' : '/'}>
        <App />
      </BrowserRouter>
    </React.StrictMode>,
  )
}

boot().catch((err) => {
  console.error('[graphdb] Failed to boot:', err)
  document.getElementById('root')!.innerHTML =
    `<div style="color:red;padding:2rem;font-family:monospace">
      Failed to initialize: ${err.message}
    </div>`
})
