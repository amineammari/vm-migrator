import { createContext, useContext, useEffect, useState } from 'react'
import { clearAuthStorage, getAccessToken, getStoredUser, setAuthStorage } from '../auth/storage'
import { fetchCurrentUser, loginUser } from '../api/auth'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [user, setUser] = useState(getStoredUser())
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    async function bootstrap() {
      if (!getAccessToken()) {
        setIsLoading(false)
        return
      }
      try {
        const currentUser = await fetchCurrentUser()
        setUser(currentUser)
        setAuthStorage({ access: getAccessToken(), refresh: localStorage.getItem('vm_migrator_refresh'), user: currentUser })
      } catch {
        clearAuthStorage()
        setUser(null)
      } finally {
        setIsLoading(false)
      }
    }

    bootstrap()
  }, [])

  async function login(credentials) {
    const data = await loginUser(credentials)
    setAuthStorage({
      access: data.access,
      refresh: data.refresh,
      user: data.user,
    })
    setUser(data.user)
    return data.user
  }

  function logout() {
    clearAuthStorage()
    setUser(null)
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        isLoading,
        isAuthenticated: Boolean(user && getAccessToken()),
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}
