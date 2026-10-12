const pendingPrefix = "rfs.pending.";
const uploadPrefix = "rfs:upload:v1:";
const actorUploadPrefix = (userId: string) =>
  `${uploadPrefix}${encodeURIComponent(userId)}:`;
export const clearActorJournals = (userId: string) => {
  sessionStorage.removeItem(pendingPrefix + userId);
  for (const key of Object.keys(sessionStorage)) {
    if (key.startsWith(actorUploadPrefix(userId)))
      sessionStorage.removeItem(key);
  }
};
export const clearOtherActorJournals = (userId: string) => {
  for (const key of Object.keys(sessionStorage)) {
    if (
      (key.startsWith(pendingPrefix) && key !== pendingPrefix + userId) ||
      (key.startsWith(uploadPrefix) &&
        !key.startsWith(actorUploadPrefix(userId)))
    ) {
      sessionStorage.removeItem(key);
    }
  }
};
