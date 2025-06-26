import { useContext } from 'react'
import DRESContext from '../context/DRESContext'

export function useConnectionStatus() {
  const { isLoggedIn } = useContext(DRESContext)

  return {
    backend: { isLoading: false, isError: false },
    dres: {
      isLoading: false,
      isError: !isLoggedIn 
    }
  }
}
