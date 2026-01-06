export type JobStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed";

export interface JobState {
  id: string;
  status: JobStatus;
  stage?: string;
  message?: string;
  startedAt: number;
  updatedAt: number;
}

const jobs = new Map<string, JobState>();

export function createJob(): JobState {
  const id = crypto.randomUUID();
  const now = Date.now();
  const job: JobState = {
    id,
    status: "pending",
    startedAt: now,
    updatedAt: now,
  };
  jobs.set(id, job);
  return job;
}

export function updateJob(
  id: string,
  update: Partial<JobState>
) {
  const job = jobs.get(id);
  if (!job) return;
  jobs.set(id, {
    ...job,
    ...update,
    updatedAt: Date.now(),
  });
}

export function getJob(id: string) {
  return jobs.get(id);
}
