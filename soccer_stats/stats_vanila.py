import csv

'''
####Assumptions####
- Goal difference is goals allowed subtracted from goals ( and not the absolute value of the difference) 

'''


class Stats():

    def __init__(self, csv_file):
        with open(csv_file, newline='') as soccer_csv:
            reader = csv.DictReader(soccer_csv)
            self.stats = []
            for row in reader:
                row['goal_diff'] = int(row['Goals']) - int(row['Goals Allowed'])
                row['win_pct'] = int(row['Wins']) / float(row['Games'])
                self.stats.append(row)

    def get_team_with_best_goal_diff(self):
        print('================ Team with max goal difference =================')
        teams_with_most_goal_diff = max(self.stats, key=lambda team: team['goal_diff'])['Team']
        print(teams_with_most_goal_diff)
        return teams_with_most_goal_diff

    def get_top_ten_teams_with_highest_win_pct(self):
        print('============= Top 10 team with highest win percentage ==========')
        teams_sorted_by_win_pct = sorted(self.stats, key=lambda team: team['win_pct'], reverse=True)
        top_10_teams = [[item['Team'], round(item['win_pct'], 2)] for item in teams_sorted_by_win_pct[:10]]
        print([print('Team: ' + team[0] + ', Win Pecentage: ' + str(team[1])) for team in top_10_teams])
        return top_10_teams

    def get_team_stats_with_most_draws(self):
        print('======================= Team with most draws ===================')
        teams_with_most_draws = max(self.stats, key=lambda team: int(team['Draws']))
        print(teams_with_most_draws)
        return teams_with_most_draws


if __name__ == "__main__":
    stats = Stats('soccer.csv')
    stats.get_team_with_best_goal_diff()
    stats.get_top_ten_teams_with_highest_win_pct()
    stats.get_team_stats_with_most_draws()
