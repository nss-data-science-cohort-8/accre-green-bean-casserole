import pandas as pd

# slurm log csv conversion
def log_to_df(log):
    df = pd.read_csv(log,
        header=None,
        delimiter=' - ',
        engine='python')
    return df

# slurm log finding for user 9204, with a return code of 1, and time of 15 seconds, and in sbatch. pulling only the timestamps out of the log
def df_to_datelist(df1):
    df = df1.copy(deep = True)
    df[3] = df[3].str.replace('time ', '')
    df[3] = df[3].astype(float)
    df = df[df[1] == 'user 9204']
    df = df[df[3] >= 15]
    df = df[df[4] == "returncode 1"]
    df['sbatch'] = df[5].apply(lambda x: 1 if 'sbatch' in x else 0)
    df = df[df['sbatch'] == 1]
    df[0] = pd.to_datetime(df[0])
    return df[0].to_list()




def count_jobs_before_interr(all_errors_func = all_errors, jobs_func = jobs, typeTime = 'h', countTime = 1, on = 'END'):
    """
    Calculates the number of jobs occurring within a specified time window 
    relative to each error timestamp, based on the relationship specified 
    (BEGIN, DURING, END, or ALL). Returns a DataFrame where each row corresponds 
    to an error and the number of jobs meeting the specified criteria.

    Parameters:
    ----------
    all_errors : pd.Series or iterable
        A list or Series of error timestamps. Each timestamp is used as 
        a reference point to count the jobs within the specified time window.

    jobs : pd.DataFrame
        A DataFrame containing job details with at least the following columns:
        - 'BEGIN': The start times of jobs.
        - 'END': The end times of jobs.

    typeTime : str, optional
        The unit of time for the countTime parameter. Accepted values are:
        - 'm': Minutes
        - 'h': Hours (default)
        - 'd': Days

    countTime : float, optional
        The size of the time window in the units specified by typeTime. 
        For example:
        - countTime=1 with typeTime='h' means a 1-hour window.
        - countTime=30 with typeTime='m' means a 30-minute window.

    on : str, optional
        Defines the relationship between the jobs and the error timestamp. 
        Accepted values are:
        - 'BEGIN': Count jobs whose start times fall within the time window 
                   before the error.
        - 'DURING': Count jobs that were active (spanning) during the error.
        - 'END': Count jobs whose end times fall within the time window 
                 before the error. (Default)
        - 'ALL': Generates a DataFrame with counts for all relationships:
            - 'Start Count': Number of jobs starting within the time window.
            - 'During Count': Number of jobs spanning the error timestamp.
            - 'End Count': Number of jobs ending within the time window.

    Returns:
    -------
    pd.DataFrame
        - For 'BEGIN', 'DURING', or 'END': A DataFrame where each row corresponds 
          to an error and its associated count of jobs based on the specified criteria.
        - For 'ALL': A DataFrame with columns 'Interruption Time', 'Start Count', 
          'During Count', and 'End Count'.

    Notes:
    -----
    - If an invalid value for `on` is provided, the function defaults to 'END' 
      and prints a warning message.
    - The 'ALL' option adds comprehensive job counts across all specified 
      relationships to the error timestamps.
    """
    
    
    
#     error_min_time = all_errors.min() - pd.Timedelta(hours=time_hours)
#         error_max_time = all_errors.max()

#          Filter jobs within the global range
#         jobs_filtered = jobs[(jobs['BEGIN'] <= error_max_time) & (jobs['END'] >= error_min_time)]
    
    time_dict = {
        'm': 60,
        'h': 1,
        'd': 1/24    
    }
    time_hours = countTime / time_dict[typeTime]
    error_min_time = min(all_errors) - pd.Timedelta(hours=time_hours)
    error_max_time = max(all_errors)
    on = on.strip().upper()
    errors_array = np.array(all_errors_func)
    all_errors_func = sorted(all_errors_func)
    
    if on == 'BEGIN':
        
        jobs_copy = jobs_func.copy(deep = True)
        jobs_copy = jobs_copy[(jobs['BEGIN'] <= error_max_time) & (jobs['BEGIN'] >= error_min_time)]
        jobs_copy = jobs_copy.sort_values('BEGIN')
        job_counts_for_interrupt = {}
        last_error_date = all_errors_func[0]
        for i, error in enumerate(tqdm(all_errors_func, desc="Processing Errors")):
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            if (error - last_error_date).days >= 30:
                jobs_copy = jobs_copy[jobs_copy['BEGIN'] >= hour_less_than_given]
                print(last_error_date)
                last_error_date = error
                
            
            count = ((jobs_copy['BEGIN'] > hour_less_than_given) & (jobs_copy['BEGIN'] <= error)).sum()
            job_counts_for_interrupt[error] = count
    
    elif on == 'DURING':
        
        jobs_copy = jobs_func.copy(deep = True)
        jobs_copy = jobs_copy[(jobs['BEGIN'] <= error_max_time) & (jobs['END'] >= error_min_time)]
        jobs_copy = jobs_copy.sort_values('END')
        job_counts_for_interrupt = {}
        last_error_date = all_errors_func[0]
        for i, error in enumerate(tqdm(all_errors_func, desc="Processing Errors")):
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            if (error - last_error_date).days >= 30:
                jobs_copy = jobs_copy[jobs_copy['END'] >= error]
                print(last_error_date)
                last_error_date = error
            #hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            count = ((jobs_copy['END'] > error) & (jobs_copy['BEGIN'] < error)).sum()
            job_counts_for_interrupt[error] = count
            
    elif on == 'END':
        
        jobs_copy = jobs_func.copy(deep = True)
        jobs_copy = jobs_copy[(jobs['END'] <= error_max_time) & (jobs['END'] >= error_min_time)]
        jobs_copy = jobs_copy.sort_values('END')
        job_counts_for_interrupt = {}
        last_error_date = all_errors_func[0]
        for i, error in enumerate(tqdm(all_errors_func, desc="Processing Errors")):
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            if (error - last_error_date).days >= 30:
                jobs_copy = jobs_copy[jobs_copy['END'] >= hour_less_than_given]
                print(last_error_date)
                last_error_date = error
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            count = ((jobs_copy['END'] > hour_less_than_given) & (jobs_copy['END'] <= error)).sum()
            job_counts_for_interrupt[error] = count
        
    elif on == 'ALL':
        
        jobs_copy = jobs_func.copy(deep = True)
        jobs_copy = jobs_copy[((jobs['END'] <= error_max_time) & (jobs['END'] >= error_min_time)) | ((jobs['BEGIN'] <= error_max_time) & (jobs['BEGIN'] >= error_min_time)) | ((jobs['BEGIN'] <= error_max_time) & (jobs['END'] >= error_min_time))]
        jobs_copy = jobs_copy.sort_values('END')
        
        job_counts_for_interrupt_begin = {}
        job_counts_for_interrupt_during = {}
        job_counts_for_interrupt_end = {}
        last_error_date = all_errors_func[0]

        for i, error in enumerate(tqdm(all_errors_func, desc="Processing Errors")):
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            if (error - last_error_date).days >= 30:
                jobs_copy = jobs_copy[jobs_copy['END'] >= hour_less_than_given]
                print(last_error_date)
                last_error_date = error

            countbegin = ((jobs_copy['BEGIN'] > hour_less_than_given) & (jobs_copy['BEGIN'] <= error)).sum()
            countduring = ((jobs_copy['END'] > error) & (jobs_copy['BEGIN'] < error)).sum()
            countend = ((jobs_copy['END'] > hour_less_than_given) & (jobs_copy['END'] <= error)).sum()
            
            job_counts_for_interrupt_begin[error] = countbegin
            job_counts_for_interrupt_during[error] = countduring
            job_counts_for_interrupt_end[error] = countend
            
        df1 =  pd.DataFrame(job_counts_for_interrupt_begin.items())
        #df.rename(columns={'A': 'a', 'B': 'c'}, inplace=True)
        df1.rename(columns = {1:'Start Count', 0:'Interruption Time'}, inplace = True)
        df1['During Count'] = job_counts_for_interrupt_during.values()
        df1['End Count'] = job_counts_for_interrupt_end.values()
        
        return df1
        
            
    else:
        
        
        
        print(f'Your "ON" variable of "{on}" was not found to be (BEGIN, END, DURING, or ALL), so defaulted to END.')
        
        jobs_copy = jobs_func.copy(deep = True)
        jobs_copy = jobs_copy[(jobs['END'] <= error_max_time) & (jobs['END'] >= error_min_time)]
        jobs_copy = jobs_copy.sort_values('END')
        job_counts_for_interrupt = {}
        last_error_date = all_errors_func[0]
        for i, error in enumerate(tqdm(all_errors_func, desc="Processing Errors")):
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            if (error - last_error_date).days >= 30:
                jobs_copy = jobs_copy[jobs_copy['END'] >= hour_less_than_given]
                print(last_error_date)
                last_error_date = error
            hour_less_than_given = error - pd.Timedelta(hours=time_hours)
            count = ((jobs_copy['END'] > hour_less_than_given) & (jobs_copy['END'] <= error)).sum()
            job_counts_for_interrupt[error] = count

        
    
    
    return pd.DataFrame(job_counts_for_interrupt.items())