import pandas as pd

pd.options.display.max_columns = 500
pd.options.display.width = 1000
'''
####Assumptions####
- Goal difference is goals allowed subtracted from goals ( and not the absolute value of the difference) 

'''


class Stats():

    def __init__(self, csv_file):
        self.soccer_df = pd.read_csv(csv_file)
        self.soccer_df['goal_diff'] = self.soccer_df['Goals'] - self.soccer_df['Goals Allowed']
        self.soccer_df['win_pct'] = self.soccer_df['Wins'] / (1.0 * self.soccer_df['Games'])

    def get_team_with_best_goal_diff(self):
        print('================ Team with max goal difference =================')
        teams_with_most_goal_diff = self.soccer_df[self.soccer_df['goal_diff'] == self.soccer_df['goal_diff'].max()]['Team']
        print(teams_with_most_goal_diff)
        return teams_with_most_goal_diff

    def get_top_ten_teams_with_highest_win_pct(self):
        print('============= Top 10 team with highest win percentage ==========')
        top_10_teams = self.soccer_df.nlargest(10, 'win_pct')[['Team', 'win_pct']]
        print(top_10_teams)
        return top_10_teams

    def get_team_stats_with_most_draws(self):
        print('======================= Team with most draws ===================')
        team_with_most_draws = self.soccer_df[self.soccer_df['Draws'] == self.soccer_df['Draws'].max()]
        print(team_with_most_draws)
        return team_with_most_draws


if __name__ == "__main__":
    stats = Stats('soccer.csv')
    stats.get_team_with_best_goal_diff()
    stats.get_top_ten_teams_with_highest_win_pct()
    stats.get_team_stats_with_most_draws()
