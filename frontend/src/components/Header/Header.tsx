import { useState } from 'react'
import { Settings } from 'lucide-react'
import { ConnectionStatus } from '../ConnectionStatus/ConnectionStatus'
import styles from './Header.module.css'
import { SearchSettingsPopup } from '../SearchSettingsPopup/SearchSettingsPopup'

export const Header: React.FC = () => {
  const [showSettings, setShowSettings] = useState(false)

  return (
    <header className={styles.header}>
      <div className={styles.inner}>
        <h1 className={styles.logo}>Image & Video Analysis</h1>
        <div className={styles.statusMenu}>
          <ConnectionStatus />
          <button
            className={styles.menuBtn}
            aria-label="Settings"
            onClick={() => setShowSettings(true)}
            style={{ marginLeft: '0.6em' }}
          >
            <Settings size={28} />
          </button>
        </div>
      </div>
      {showSettings && (
        <SearchSettingsPopup onClose={() => setShowSettings(false)} />
      )}
    </header>
  )
}

