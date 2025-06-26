import React from 'react'

// Stub provider, no React Query used in UI-only mode
export const ReactQueryProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <>{children}</>
)
