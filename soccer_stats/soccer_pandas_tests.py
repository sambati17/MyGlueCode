import unittest
import stats_pandas as sp


class TestStats(unittest.TestCase):

    def setUp(self):
        self.stats = sp.Stats('soccer_test.csv')

    def test_get_team_with_best_goal_diff(self):
        team_with_best_goal_diff = self.stats.get_team_with_best_goal_diff()
        self.assertEqual(team_with_best_goal_diff.values[0], 'Manchester United')

    def test_get_top_ten_teams_with_highest_win_pct(self):
        top_ten_teams = self.stats.get_top_ten_teams_with_highest_win_pct()
        self.assertEqual(top_ten_teams.values[0][0], 'Arsenal')
        self.assertEqual(top_ten_teams.values[5][0], 'Chelsea')
        self.assertEqual(top_ten_teams.values[9][0], 'Blackburn')

    def test_get_team_stats_with_most_draws(self):
        team_with_most_draws = self.stats.get_team_stats_with_most_draws()
        self.assertEqual(team_with_most_draws['Team'].values[0], 'Derby')
        self.assertEqual(team_with_most_draws['Games'].values[0], 38)
        self.assertEqual(team_with_most_draws['Wins'].values[0], 8)
        self.assertEqual(team_with_most_draws['Losses'].values[0], 6)
        self.assertEqual(team_with_most_draws['Draws'].values[0], 24)
        self.assertEqual(team_with_most_draws['Goals'].values[0], 33)
        self.assertEqual(team_with_most_draws['Goals Allowed'].values[0], 63)
        self.assertEqual(team_with_most_draws['Points'].values[0], 30)

    def test_invalid_file(self):
        self.assertRaises(FileNotFoundError, sp.Stats, 'invalid.csv')


if __name__ == '__main__':
    unittest.main()
