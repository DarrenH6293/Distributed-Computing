import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def load_pop_data(population_csv):
    state_abbr = {
        "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar", "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de", "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id", "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks", "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md", "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms", "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv", "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm", "new york": "ny", "north carolina": "nc", "north dakota": "nd", "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc", "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt", "virginia": "va", "washington": "wa", "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy"
    }
    
    df = pd.read_csv(population_csv)
    
    state_col = ["NAME"]
    df["state"] = df[state_col].astype(str).str.strip().str.lower().map(state_abbr)
    df = df.dropna(subset=["state"])
    
    return df

def did(file_path, treated_states, control_states, pre_year, post_year):
    query = f"""
        WITH turnout AS (
            SELECT 
                state, 
                year, 
                SUM(votes) AS total_votes
            FROM read_parquet('{file_path}')
            WHERE state IN ({','.join([f'\'{s}\'' for s in treated_states + control_states])})
            GROUP BY state, year
        )
        SELECT 
            state,
            AVG(CASE WHEN year = {pre_year} THEN total_votes ELSE NULL END) AS pre_turnout,
            AVG(CASE WHEN year = {post_year} THEN total_votes ELSE NULL END) AS post_turnout
        FROM turnout
        GROUP BY state
    """
    df = duckdb.query(query).to_df()
    df["turnout_change"] = df["post_turnout"] - df["pre_turnout"]
    
    print("Before and After Turnout per State:")
    for index, row in df.iterrows():
        print(f"State: {row['state']}, {pre_year}: {row['pre_turnout']}, {post_year}: {row['post_turnout']}")
    
    treated_avg_change = df[df["state"].isin(treated_states)]["turnout_change"].mean()
    control_avg_change = df[df["state"].isin(control_states)]["turnout_change"].mean()
    did_effect = treated_avg_change - control_avg_change
    return df, did_effect

def analyze_bg_vs_nbg(file_path, population_csv, battleground_states, year):
    pop_df = load_pop_data(population_csv)
    
    all_states = set(pop_df["state"].tolist())
    non_battleground_states = list(all_states - set(battleground_states))
    
    query = f"""
        SELECT 
            state, 
            SUM(votes) AS total_votes
        FROM read_parquet('{file_path}')
        WHERE state IN ({','.join([f'\'{s}\'' for s in all_states])})
            AND year = {year}
            AND office = 'President'
        GROUP BY state
        HAVING SUM(votes) > 0
        ORDER BY state
    """
    turnout_df = duckdb.query(query).to_df()

    turnout_df = turnout_df.merge(pop_df, on="state", how="left")
    turnout_df["turnout_rate"] = turnout_df["total_votes"] / turnout_df[f"POPESTIMATE{year}"]
    turnout_df.drop(columns=[f"POPESTIMATE{year}"], inplace=True)
    
    turnout_df = turnout_df[np.abs(turnout_df["turnout_rate"] - turnout_df["turnout_rate"].mean()) <= (3 * turnout_df["turnout_rate"].std())]
    
    battleground_df = turnout_df[turnout_df["state"].isin(battleground_states)]
    non_battleground_df = turnout_df[turnout_df["state"].isin(non_battleground_states)]
    
    battleground_mean = battleground_df["turnout_rate"].mean()
    non_battleground_mean = non_battleground_df["turnout_rate"].mean()
    
    print(f"Year: {year}:")
    print(f"Battleground States Mean Turnout Rate: {battleground_mean:.4f}")
    print(f"Non-Battleground States Mean Turnout Rate: {non_battleground_mean:.4f}")
    
    return battleground_mean, non_battleground_mean

def plot_did(did_df, treated_states, control_states, pre_year, post_year):
    did_df["group"] = did_df["state"].apply(lambda x: "Treated" if x in treated_states else "Control")

    agg_df = did_df.groupby("group")[["pre_turnout", "post_turnout"]].mean().reset_index()
    agg_df = agg_df.melt(id_vars=["group"], value_vars=["pre_turnout", "post_turnout"],
                          var_name="Period", value_name="Turnout")
    agg_df["Period"] = agg_df["Period"].map({"pre_turnout": f"{pre_year}", "post_turnout": f"{post_year}"})

    plt.figure(figsize=(8, 6))
    sns.barplot(data=agg_df, x="Period", y="Turnout", hue="group", ci=None)

    plt.title(f"Difference-in-Differences Effect ({pre_year} vs {post_year})")
    plt.xlabel("Period")
    plt.ylabel("Average Turnout")
    plt.legend(title="Group")
    plt.grid(axis="y")
    plt.show()

def main():
    file_path = "election_results_cleaned.parquet"
    state_pop_file_path = "state_populations.csv"

    treated_states = ["al", "ga", "ky"]
    control_states = ["wa", "mn", "ct"]
    pre_year, post_year = 2012, 2016
    did_df, did_effect = did(file_path, treated_states, control_states, pre_year, post_year)
    print(f"Difference-in-Differences effect: {did_effect}")
    plot_did(did_df, treated_states, control_states, pre_year, post_year)

    battleground_states_2012 = ["wi", "ia", "pa", "mi", "nc", "nh"]
    analyze_bg_vs_nbg(file_path, state_pop_file_path, battleground_states_2012, 2012)
    battleground_states_2016 = ["wi", "az", "pa", "mi", "nv", "co", "nc"]
    analyze_bg_vs_nbg(file_path, state_pop_file_path, battleground_states_2016, 2016)

if __name__ == "__main__":
    main()
