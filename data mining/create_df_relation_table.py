from create_df_from_sql import *

def main(task_id):

    file_path = f"/home/reutme/Big_data/final_project/data/works_df_{task_id}.parquet"

    df = vaex.open(file_path)

    work_id_list = df['work_id'].tolist()

    output_file = f'work_inst_auth_df_{task_id}'

    proccess_queries_chunks(work_id_list, save_file_name=output_file)



if __name__ == "__main__":
    # Get the SLURM_ARRAY_TASK_ID from the command line
    task_id = int(sys.argv[1])
    main(task_id)


