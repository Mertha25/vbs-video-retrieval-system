const BASE_URL = "https://vbs.videobrowsing.org"
const USERNAME = 'TECHtalent13'
const PASSWORD = 'QbaA6G4P'

const EVALUATION_NAME = 'IVADL2025' 


interface ILoginResponse {
    id: string, 
    username: string, 
    role: string, 
    sessionId: string
}

const login = async (): Promise<ILoginResponse | null> => {
    try {
        const res = await fetch(`${BASE_URL}/api/v2/login`, {
            method: "POST",
            headers: new Headers({
                'content-type': 'application/json'
            }),
            body: JSON.stringify({
                username: USERNAME,
                password: PASSWORD
            })
        })
        
        const data = await res.json()

        if (res.status === 200) {
            return data as ILoginResponse
        }

        console.log(data)
        return null
    } catch (err) {
        console.log(err)
        return null
    }
}


interface IEvaluationInfoListResponse {
    id: string
    name: string
    type: "SYNCHRONOUS"
    status: "CREATED"
    templateId: string
    templateDescription: string
    teams: [
        string
    ]
    taskTemplates: [
        {
            name: string
            taskGroup: string
            taskType: string
            duration: 0
        }
    ]
}


const evaluationInfoList = async (sessionId: string):Promise<IEvaluationInfoListResponse | null> => {
    try {
        const res = await fetch(`${BASE_URL}/api/v2/client/evaluation/list?session=${sessionId}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
            },
        })

        if (res.status === 200) {
            const data = await res.json() as IEvaluationInfoListResponse[]
            const evaluation = data.filter((e) => e.name === EVALUATION_NAME)
            
            if (evaluation.length !== 1) {
                console.log(data)
                console.log("Evaluation not found")
                return null
            }

            return evaluation[0]
        }

        const data = await res.json()
        console.log(data)
        return null
    } catch (err) {
        console.log(err)
        return null
    }
}


interface ISubmissionResult {
    status: boolean
    submission?: string
    description: string
}

const submit = async (sessionId: string, evaluationId: string, videoId: string, timestampInSec: number) => {
    try {
        const res = await fetch(`${BASE_URL}/api/v2/submit/${evaluationId}?session=${sessionId}`, {
            method: "POST",
            body: JSON.stringify({
                answerSets: [
                    {
                        taskId: evaluationId,
                        taskName: EVALUATION_NAME,
                        answers: [
                            {
                                mediaItemName: videoId,
                                mediaItemCollectionName: "IVADL",
                                start: timestampInSec * 1000,
                                end: timestampInSec * 1000
                            }
                        ]
                    }
                ]
            })
        })
        const data = await res.json()
        return data as ISubmissionResult
    } catch (err) {
        console.log(err)
        return null
    }
}

const DRES_API = {
    login,
    evaluationInfoList,
    submit
}

export default DRES_API