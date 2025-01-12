from etl import get_final_merged_df

if __name__ == "__main__":
    df = get_final_merged_df(
        base_dir="./",
        pl_tables_csv="./pl-tables-1993-2024.csv",
        who_scored_csv="./premier_league_stats.csv"
    )

    df.to_csv('final_output.csv',index=False, encoding="utf-8")
    print(df)

