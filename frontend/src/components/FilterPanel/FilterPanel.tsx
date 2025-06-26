import React, { useRef, useState, useEffect } from 'react'
import { Search, RotateCw, X } from 'lucide-react'
import styles from './FilterPanel.module.css'

export interface FilterCriteria {
  text?: string
  color?: string
  file?: File | null
  objects?: string[]
  words?: string
  interval?: { from: string; to: string }
  embedding?: number[]
}

/* util */
const clamp = (n: number, min: number, max: number) =>
  Math.max(min, Math.min(max, n))
const toSec = (t: string) => {
  const [h = '0', m = '0', s = '0'] = t.split(':')
  return +h * 3600 + +m * 60 + +s
}

export const FilterPanel: React.FC<{ onChange: (c: FilterCriteria) => void }> = ({ onChange }) => {
  /* toggles */
  const [enableText, setEnableText] = useState(true)
  const [enableColor, setEnableColor] = useState(false)
  const [enableFile, setEnableFile] = useState(false)
  const [enableObjects, setEnableObjects] = useState(false)
  const [enableWords, setEnableWords] = useState(false)
  const [enableInterval, setEnableInterval] = useState(false)

  /* values */
  const [text, setText] = useState('')
  const [color, setColor] = useState('#0ea5e9')
  const [file, setFile] = useState<File | null>(null)
  const [objects, setObjects] = useState<string[]>([])
  const [objectDraft, setObjectDraft] = useState('')
  const [words, setWords] = useState('')
  const [intervalFrom, setIntervalFrom] = useState('00:00:00')
  const [intervalTo, setIntervalTo] = useState('00:00:00')
  const [intervalError, setIntervalError] = useState<string | null>(null)

  /* interval validation */
  useEffect(() => {
    if (enableInterval) {
      setIntervalError(toSec(intervalFrom) < toSec(intervalTo) ? null : 'Start must be before End')
    } else setIntervalError(null)
  }, [enableInterval, intervalFrom, intervalTo])

  /* file helpers */
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [dragActive, setDragActive] = useState(false)
  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => setFile(e.target.files?.[0] ?? null)
  const onDragOver = (e: React.DragEvent) => { e.preventDefault(); setDragActive(true) }
  const onDragLeave = () => setDragActive(false)
  const onDrop = (e: React.DragEvent) => { e.preventDefault(); setDragActive(false); if (e.dataTransfer.files.length) setFile(e.dataTransfer.files[0]); setEnableFile(true) }

  /* object helpers */
  const addObject = (v: string) => { const val = v.trim(); if (!val || objects.includes(val)) return; setObjects(o => [...o, val]); setObjectDraft('') }
  const removeObject = (v: string) => setObjects(o => o.filter(i => i !== v))
  const onObjectKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') { e.preventDefault(); addObject(objectDraft) }
    else if (e.key === 'Backspace' && !objectDraft && objects.length) removeObject(objects[objects.length - 1])
  }

  /* interval helpers */
  const setPart = (
    base: string,
    idx: 0 | 1 | 2,
    max: number,
    setter: React.Dispatch<React.SetStateAction<string>>
  ) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const raw = e.target.value.replace(/\D+/g, '')
    const num = raw === '' ? '' : clamp(+raw, 0, max).toString()
    const parts = base.split(':')
    parts[idx] = num
    setter(parts.join(':'))
  }
  const onBlurPart = (
    base: string,
    idx: 0 | 1 | 2,
    setter: React.Dispatch<React.SetStateAction<string>>
  ) => () => {
    const parts = base.split(':')
    if (parts[idx] === '') parts[idx] = '00'
    setter(parts.map(p => p.padStart(2, '0')).join(':'))
  }

  /* criteria */
  const buildCriteria = (): FilterCriteria => ({
    ...(enableText && text.trim() && { text: text.trim() }),
    ...(enableColor && { color }),
    ...(enableObjects && objects.length && { objects }),
    ...(enableWords && words.trim() && { words: words.trim() }),
    ...(enableInterval && !intervalError && { interval: { from: intervalFrom, to: intervalTo } }),
  })

  const handleSearch = () => onChange(buildCriteria())
  const handleReset = () => {
    setText('');
    setColor('#0ea5e9');
    setFile(null);
    setObjects([]);
    setWords('');
    setIntervalFrom('00:00:00');
    setIntervalTo('00:00:00');
    setEnableText(true);
    setEnableColor(false);
    setEnableFile(false);
    setEnableObjects(false);
    setEnableWords(false);
    setEnableInterval(false);

    // Notify parent of reset criteria
    onChange(buildCriteria());
  }


  /* ─── render ─── */
  return (
    <div className={styles.panelRoot}>
      {/* TEXT */}
      <div className={styles.filterRow}>
        <label>
          <input
            type="checkbox"
            checked={enableText}
            onChange={() => setEnableText(!enableText)}
          />
          <span style={{ marginLeft: 8, fontWeight: 600 }}>
            Enable Text Query
          </span>
        </label>
        {enableText && (
          <textarea
            value={text}
            onChange={e => setText(e.target.value)}
            className={styles.textInput}
            placeholder="Type to Query…"
            rows={3}
            style={{ marginTop: 8, resize: 'vertical' }}
          />
        )}
      </div>

      {/* COLOR */}
      <div className={styles.filterRow}>
        <label>
          <input
            type="checkbox"
            checked={enableColor}
            onChange={() => setEnableColor(!enableColor)}
          />
          <span style={{ marginLeft: 8, fontWeight: 600 }}>
            Enable Color Query
          </span>
        </label>
        {enableColor && (
          <input
            type="color"
            value={color}
            onChange={e => setColor(e.target.value)}
            className={styles.colorInput}
          />
        )}
      </div>

      {/* MEDIA */}
      <div className={styles.filterRow}>
        <label>
          <input
            type="checkbox"
            checked={enableFile}
            onChange={() => setEnableFile(!enableFile)}
          />
          <span style={{ marginLeft: 8, fontWeight: 600 }}>
            Enable Media Query
          </span>
        </label>
        {enableFile && (
          <>
            <div className={styles.fileRow}>
              <input
                id="file-upload"
                type="file"
                accept="video/*,image/*"
                ref={fileInputRef}
                onChange={onFileChange}
                className={styles.fileInput}
              />
              <label htmlFor="file-upload" className={styles.fileLabel}>
                Upload Video/Photo
              </label>
              {file && <span className={styles.fileName}>{file.name}</span>}
            </div>
            <div
              className={`${styles.dragZone} ${
                dragActive ? styles.dragZoneActive : ''
              }`}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
              tabIndex={0}
            >
              {dragActive
                ? 'Drop your file here!'
                : 'Or drag & drop a file here'}
            </div>
          </>
        )}
      </div>

      {/* OBJECTS */}
      <div className={styles.filterRow}>
        <label>
          <input
            type="checkbox"
            checked={enableObjects}
            onChange={() => setEnableObjects(!enableObjects)}
          />
          <span style={{ marginLeft: 8, fontWeight: 600 }}>
            Enable Object Query
          </span>
        </label>
        {enableObjects && (
          <div className={styles.objectBox}>
            <div className={styles.objectTags}>
              {objects.map(o => (
                <span key={o} className={styles.tag}>
                  {o}
                  <button
                    type="button"
                    className={styles.tagRemove}
                    onClick={() => removeObject(o)}
                  >
                    <X size={12} />
                  </button>
                </span>
              ))}
              <input
                value={objectDraft}
                onChange={e => setObjectDraft(e.target.value)}
                onKeyDown={onObjectKey}
                placeholder={objects.length ? '' : 'Type object + Enter…'}
                className={styles.objectInput}
              />
            </div>
          </div>
        )}
      </div>

      {/* WORDS */}
      <div className={styles.filterRow}>
        <label>
          <input
            type="checkbox"
            checked={enableWords}
            onChange={() => setEnableWords(!enableWords)}
          />
          <span style={{ marginLeft: 8, fontWeight: 600 }}>
            Enable Word Query
          </span>
        </label>
        {enableWords && (
          <textarea
            value={words}
            onChange={e => setWords(e.target.value)}
            className={styles.textInput}
            placeholder="Word Query…"
            rows={3}
            style={{ marginTop: 8, resize: 'vertical' }}
          />
        )}
      </div>

    {/* TIME INTERVAL */}
    <div className={styles.filterRow}>
      <label>
        <input
          type="checkbox"
          checked={enableInterval}
          onChange={() => setEnableInterval(!enableInterval)}
        />
        <span className={styles.intervalToggleTxt}>Enable Time Interval</span>
      </label>

      {enableInterval && (
        <>
          {/* FROM */}
          <div className={styles.intervalRow}>From</div>
          <div className={styles.timeGroup}>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="23"
                placeholder="HH"
                value={intervalFrom.split(':')[0] || ''}
                onChange={setPart(intervalFrom, 0, 23, setIntervalFrom)}
                onBlur={onBlurPart(intervalFrom, 0, setIntervalFrom)}
              />
              <span className={styles.unit}>h</span>
            </div>
            <span className={styles.sep}>:</span>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="59"
                placeholder="MM"
                value={intervalFrom.split(':')[1] || ''}
                onChange={setPart(intervalFrom, 1, 59, setIntervalFrom)}
                onBlur={onBlurPart(intervalFrom, 1, setIntervalFrom)}
              />
              <span className={styles.unit}>m</span>
            </div>
            <span className={styles.sep}>:</span>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="59"
                placeholder="SS"
                value={intervalFrom.split(':')[2] || ''}
                onChange={setPart(intervalFrom, 2, 59, setIntervalFrom)}
                onBlur={onBlurPart(intervalFrom, 2, setIntervalFrom)}
              />
              <span className={styles.unit}>s</span>
            </div>
          </div>

          {/* TO */}
          <div className={styles.intervalRow} style={{ marginTop: '.6rem' }}>To</div>
          <div className={styles.timeGroup}>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="23"
                placeholder="HH"
                value={intervalTo.split(':')[0] || ''}
                onChange={setPart(intervalTo, 0, 23, setIntervalTo)}
                onBlur={onBlurPart(intervalTo, 0, setIntervalTo)}
              />
              <span className={styles.unit}>h</span>
            </div>
            <span className={styles.sep}>:</span>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="59"
                placeholder="MM"
                value={intervalTo.split(':')[1] || ''}
                onChange={setPart(intervalTo, 1, 59, setIntervalTo)}
                onBlur={onBlurPart(intervalTo, 1, setIntervalTo)}
              />
              <span className={styles.unit}>m</span>
            </div>
            <span className={styles.sep}>:</span>
            <div className={styles.timeField}>
              <input
                type="number"
                min="0"
                max="59"
                placeholder="SS"
                value={intervalTo.split(':')[2] || ''}
                onChange={setPart(intervalTo, 2, 59, setIntervalTo)}
                onBlur={onBlurPart(intervalTo, 2, setIntervalTo)}
              />
              <span className={styles.unit}>s</span>
            </div>
          </div>

          {intervalError && <p className={styles.error}>{intervalError}</p>}
        </>
      )}
    </div>

    {/* ACTIONS */}
    <div className={styles.filterActions}>
      <button type="button" className={styles.btnReset} onClick={handleReset}>
        <RotateCw size={18} style={{ marginRight: 8 }} />
        Reset
      </button>
      <button type="button" className={styles.btnSearch} onClick={handleSearch}>
        <Search size={18} style={{ marginRight: 8 }} />
        Search
      </button>
    </div>

  </div>
)};