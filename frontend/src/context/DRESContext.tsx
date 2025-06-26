import { FC, useState, createContext, useEffect, useRef } from 'react';
import { notifications } from '@mantine/notifications';
import DRES_API from '../dresApi';
import { IDRES } from '../interfaces/IDRES';

const DRESContext = createContext<IDRES>({
  isLoggedIn: false,
  submit: (videoId, timestampInSec) => {
    console.log('init');
  },
});

export default DRESContext;

interface IDresInfo {
  sessionId: string;
  evaluationId: string;
}

type DRESProviderProps = {
  children: React.ReactNode;
};

export const DRESProvider: FC<DRESProviderProps> = ({ children }) => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const dresInfo = useRef<IDresInfo | null>(null);

useEffect(() => {
  (async () => {
    console.log("🔁 Trying to log in to DRES...");
    const loginRes = await DRES_API.login();
    console.log("🔐 Login response:", loginRes);

    if (!loginRes) {
      console.warn("❌ Login failed");
      setIsLoggedIn(false);
      return;
    }

    const evalRes = await DRES_API.evaluationInfoList(loginRes.sessionId);
    console.log("📋 Evaluation response:", evalRes);

    if (!evalRes) {
      console.warn("❌ Evaluation not found");
      setIsLoggedIn(false);
      return;
    }

    dresInfo.current = {
      sessionId: loginRes.sessionId,
      evaluationId: evalRes.id,
    };
    console.log("✅ Connected to DRES");
    setIsLoggedIn(true);
  })();
}, []);


  const _handleSubmit = (videoId: string, timestampInSec: number): void => {
    const info = dresInfo.current;
    if (!info) return;

    (async () => {
      notifications.show({
        title: 'Submission sent',
        message: `VideoId: ${videoId} @ ${timestampInSec.toFixed(2)} s`,
      });

      const result = await DRES_API.submit(
        info.sessionId,
        info.evaluationId,
        videoId,
        timestampInSec
      );

      notifications.show({
        title: 'Result',
        message: result?.description ?? 'No response',
        color: result?.submission === 'CORRECT' ? 'green' : 'red',
      });
    })().catch(console.error);
  };

  return (
    <DRESContext.Provider value={{ isLoggedIn, submit: _handleSubmit }}>
      {children}
    </DRESContext.Provider>
  );
};
