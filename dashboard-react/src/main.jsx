import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { BenchmarkProvider } from './data/BenchmarkContext.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BenchmarkProvider>
      <App />
    </BenchmarkProvider>
  </StrictMode>,
)
