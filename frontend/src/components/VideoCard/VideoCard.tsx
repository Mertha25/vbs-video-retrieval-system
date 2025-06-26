// src/components/VideoCard/VideoCard.tsx
import React, { useState, useRef, useContext } from 'react';
import DRESContext from '../../context/DRESContext';
import styles from './VideoCard.module.css';

/* ────────── Types ────────── */
export interface Video {
  id: string | number;
  title: string;
  video_path: string;
  duration: number;
  score?: number;
  timestamp?: number;
  objects?: string[];
  text?: string[];
  dominant_colors?: [number, number, number][];
}

interface SubmissionResult {
  success: boolean;
  message: string;
}

/* ────────── Component ────────── */
export const VideoCard: React.FC<{
  video: Video;
  onSubmit?: (id: string | number, ts: number) => void; // optional
  enableDRES?: boolean;                                 // true = DRES button
}> = ({ video, onSubmit, enableDRES = true }) => {
  /* local state */
  const [showPlayer, setShowPlayer] = useState(false);
  const [showSubmit, setShowSubmit] = useState(false);
  const [ts, setTs]                   = useState(video.timestamp ?? 0);
  const [submitting, setSubmitting]   = useState(false);
  const [result, setResult]           = useState<SubmissionResult | null>(null);
  const videoRef = useRef<HTMLVideoElement | null>(null);

  /* DRES context */
  const dres  = useContext(DRESContext);

  /* helpers */
  const closePlayer = () => setShowPlayer(false);
  const closeSubmit = () => {
    setTs(video.timestamp ?? 0);
    setShowSubmit(false);
    setResult(null);
  };
  const videoUrl    = video.video_path;
  const videoId     = video.video_path.split('/').pop()?.replace('.mp4', '') || video.id.toString();

  /* submission handler */
  const handleSubmit = async () => {
    /* → DRES mode if enabled + logged in */
    if (enableDRES && dres.isLoggedIn) {
      setSubmitting(true);
      try {
        await dres.submit(videoId, ts);
        setResult({ success: true, message: 'Submitted to DRES ✅' });
        setTimeout(closeSubmit, 1800);
      } catch (err) {
        console.error(err);
        setResult({ success: false, message: 'Failed to submit ❌' });
      } finally {
        setSubmitting(false);
      }
    } else {
      /* → fallback to local handler */
      onSubmit?.(video.id, ts);
      closeSubmit();
    }
  };

  /* ────────── Render ────────── */
  return (
    <>
      {/* ═══ PLAYER OVERLAY ═══ */}
      {showPlayer && (
        <div className={styles.overlay}>
          <div className={styles.modal}>
            <button className={styles.close} onClick={closePlayer}>×</button>
            <video
              autoPlay
              controls
              onPlay={e => { if (video.timestamp) e.currentTarget.currentTime = video.timestamp - 0.5; }}
              style={{ width: '100%', borderRadius: 8, background: '#000' }}
            >
              <source src={videoUrl} type="video/mp4" />
            </video>
          </div>
        </div>
      )}

      {/* ═══ SUBMIT OVERLAY ═══ */}
      {showSubmit && (
        <div className={styles.overlay}>
          <div className={styles.modal}>
            <button className={styles.close} onClick={closeSubmit}>×</button>

            <video
              ref={videoRef}
              autoPlay
              controls
              onPlay={e => { if (video.timestamp) e.currentTarget.currentTime = video.timestamp - 0.5; }}
              style={{ width: '100%', borderRadius: 8, background: '#000', marginBottom: 12 }}
            >
              <source src={videoUrl} type="video/mp4" />
            </video>

            <button className={styles.small} onClick={() => setTs(videoRef.current?.currentTime ?? ts)}>
              Update timestamp
            </button>

            <div className={styles.tsRow}>
              <span>Timestamp&nbsp;(s)</span>
              <input
                type="number"
                step="0.01"
                min="0"
                value={ts}
                onChange={e => {
                  const v = parseFloat(e.target.value);
                  videoRef.current?.fastSeek?.(v);
                  setTs(v);
                }}
              />
            </div>

            {/* feedback */}
            {result && (
              <div className={`${styles.submissionResult} ${result.success ? styles.success : styles.error}`}>
                {result.message}
              </div>
            )}

            <button
              className={styles.submitBtn}
              onClick={handleSubmit}
              disabled={
                submitting ||
                (enableDRES && !dres.isLoggedIn)  // button disabled if not logged in
              }
            >
              {submitting
                ? 'Submitting…'
                : enableDRES
                ? 'Submit to DRES'
                : 'Submit'}
            </button>
          </div>
        </div>
      )}

      {/* ═══ CARD ═══ */}
      <div className={styles.card}>
        <video src={videoUrl} controls width="100%" style={{ borderRadius: 8, background: '#000' }} />
        <div className={styles.title}>{video.title}</div>

        {video.score !== undefined        && <p className={styles.meta}>Score: {video.score.toFixed(3)}</p>}
        {video.timestamp !== undefined    && <p className={styles.meta}>Timestamp: {video.timestamp.toFixed(3)} s</p>}
        {video.objects?.length            && <p className={styles.meta}><strong>Objects:</strong> {video.objects.join(', ')}</p>}
        {video.text?.length               && <p className={styles.meta}><strong>Text:</strong> {video.text.join(', ')}</p>}
        {video.dominant_colors?.length    && (
          <div className={styles.colorRow}>
            {video.dominant_colors.map((c, i) => (
              <span
                key={i}
                className={styles.colorDot}
                title={`rgb(${c[0]}, ${c[1]}, ${c[2]})`}
                style={{ background: `rgb(${c[0]}, ${c[1]}, ${c[2]})` }}
              />
            ))}
          </div>
        )}

        <div className={styles.actionsRow}>
          <button className={styles.small} onClick={() => setShowPlayer(true)}>Play</button>
          {(onSubmit || enableDRES) && (
            <button className={styles.small} onClick={() => setShowSubmit(true)}>
              {enableDRES ? 'Submit to DRES' : 'Submit'}
            </button>
          )}
        </div>
      </div>
    </>
  );
};
