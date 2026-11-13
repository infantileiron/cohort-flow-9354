export type Dataset={id:string;name:string;source:string;row_count:number;learners:number;weeks:number[]};
export type Cohort={id:string;name:string;dataset_id:string;observation_end:number;include_completed:boolean};
export type RetentionCell={age:number;calendar_week:number;eligible:number;observed:number;active:number;missing:number;censored:number;retention:number|null};
export type Run={id:string;cohort_id:string;status:string;error:string;error_code?:string;attempts?:number;max_attempts?:number;input_cohort?:Record<string,unknown>;worker_id?:string;lease_expires_at?:string;queued_at?:string;started_at?:string;finished_at?:string;result:{learners:number;events:number;observation_end:number;cohorts:{cohort_week:number;size:number;cells:RetentionCell[]}[];markov:{states:string[];counts:number[][];matrix:number[][];expected_weeks:number[]|null;absorption:number[][]|null;identifiable:boolean;warning:string};limitations:string[];checksum:string}};
export type Review={id:string;run_id:string;comment:string;status:string;reviewer:string};
export type Report={id:string;run_id:string;review_id:string;title:string};
export type User={email:string;name:string;role:string};
