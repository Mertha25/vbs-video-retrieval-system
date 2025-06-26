import React from 'react'
import styles from './SearchSettingsPopup.module.css'

export const SearchSettingsPopup: React.FC<{ onClose: () => void }> = ({ onClose }) => (
  <div className={styles.overlay}>
    <div className={styles.popup}>
      <button className={styles.closeBtn} onClick={onClose}>×</button>
      <h2 className={styles.title}>Search Settings</h2>
      <hr style={{margin: '0.6em 0 1.3em 0'}} />
      <section>
        <h3>General</h3>
        <label>
          Max Results
          <input type="number" defaultValue={100} min={1} max={999} />
        </label>
      </section>
      <section>
        <h3>Text Query</h3>
        <label>
          Max Text Similarity
          <input type="number" defaultValue={0.9} step={0.01} min={0} max={1} />
        </label>
      </section>
      <section>
        <h3>Color Query</h3>
        <label>
          Color Radius
          <input type="number" defaultValue={4} min={1} max={10} />
        </label>
      </section>
      <section>
        <h3>Image Query</h3>
        <label>
          Max Image Similarity
          <input type="number" defaultValue={0.9} step={0.01} min={0} max={1} />
        </label>
      </section>

    </div>
  </div>
)
