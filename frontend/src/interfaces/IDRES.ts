export interface IDRES {
    isLoggedIn: boolean
    submit(videoId: string, timestampInSec: number): void
}

export interface IDRESContext {
    dres: IDRES
    update(dres: IDRES): void,
}