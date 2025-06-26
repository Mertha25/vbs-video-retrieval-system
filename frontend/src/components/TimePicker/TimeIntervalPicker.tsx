import React, { useEffect, useState } from 'react';
import styles from './TimeIntervalPicker.module.css';   // ← adjust if path differs

/* helpers */
const clamp = (n: number, min: number, max: number) =>
  Math.max(min, Math.min(max, n));
const toSec = (t: string) => {
  const [h = '0', m = '0', s = '0'] = t.split(':');
  return +h * 3600 + +m * 60 + +s;
};

export interface IntervalValue {
  from: string;          // "HH:MM:SS"
  to:   string;
}
export interface TimeIntervalPickerProps {
  value: IntervalValue;
  onChange: (v: IntervalValue, isValid: boolean) => void;
  label?: string;        // checkbox label (optional)
}

export const TimeIntervalPicker: React.FC<TimeIntervalPickerProps> = ({
  value,
  onChange,
  label = 'Enable Time Interval',
}) => {
  /* local state */
  const [enabled, setEnabled] = useState(false);
  const [from, setFrom] = useState(value.from || '00:00:00');
  const [to,   setTo]   = useState(value.to   || '00:00:00');
  const [error, setError] = useState<string | null>(null);

  /* toggle → default to 00:00:00 */
  const toggle = () => {
    const next = !enabled;
    setEnabled(next);
    if (next && (!from || !to)) {
      setFrom('00:00:00');
      setTo('00:00:00');
    }
  };

  /* push value + validity upward */
  useEffect(() => {
    if (enabled) {
      const ok = toSec(from) < toSec(to);
      setError(ok ? null : 'Start must be before End');
      onChange({ from, to }, ok);
    } else {
      setError(null);
      onChange({ from, to }, false);
    }
  }, [enabled, from, to, onChange]);

  /* build handlers for each part (HH / MM / SS) */
  const setPart =
    (
      which: 'from' | 'to',
      idx: 0 | 1 | 2,
      max: number
    ) => (e: React.ChangeEvent<HTMLInputElement>) => {
      const raw = e.target.value.replace(/\\D+/g, '');
      const num = raw === '' ? '' : clamp(+raw, 0, max).toString();
      const current = which === 'from' ? from : to;
      const parts = current.split(':');
      parts[idx] = num;
      const joined = parts.map(p => (p === '' ? '' : p.padStart(2, '0'))).join(':');
      which === 'from' ? setFrom(joined) : setTo(joined);
    };

  const blurPart =
    (
      which: 'from' | 'to',
      idx: 0 | 1 | 2
    ) => () => {
      const current = which === 'from' ? from : to;
      const parts = current.split(':');
      if (parts[idx] === '') parts[idx] = '00';
      (which === 'from' ? setFrom : setTo)(parts.join(':'));
    };

  const Row = ({
    title,
    base,
    which,
  }: {
    title: string;
    base: string;
    which: 'from' | 'to';
  }) => {
    const [h = '', m = '', s = ''] = base.split(':');
    return (
      <>
        <div className={styles.intervalRow}>{title}</div>
        <div className={styles.timeGroup}>
          <input
            type="number"
            min={0} max={23} placeholder="HH"
            value={h}
            onChange={setPart(which, 0, 23)}
            onBlur={blurPart(which, 0)}
            className={styles.timeInput}
          />
          <span>:</span>
          <input
            type="number"
            min={0} max={59} placeholder="MM"
            value={m}
            onChange={setPart(which, 1, 59)}
            onBlur={blurPart(which, 1)}
            className={styles.timeInput}
          />
          <span>:</span>
          <input
            type="number"
            min={0} max={59} placeholder="SS"
            value={s}
            onChange={setPart(which, 2, 59)}
            onBlur={blurPart(which, 2)}
            className={styles.timeInput}
          />
        </div>
      </>
    );
  };

  return (
    <div className={styles.filterRow}>
      <label>
        <input type="checkbox" checked={enabled} onChange={toggle} />
        <span style={{ marginLeft: 8, fontWeight: 600 }}>{label}</span>
      </label>

      {enabled && (
        <>
          <Row title="From" base={from} which="from" />
          <Row title="To"   base={to}   which="to" />
          {error && <p className={styles.error}>{error}</p>}
        </>
      )}
    </div>
  );
};
