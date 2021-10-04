import unittest
import stats_vanila as sv


class TestStats(unittest.TestCase):

    def setUp(self):
        self.stats = sv.Stats('soccer_test.csv')

    def test_get_team_with_best_goal_diff(self):
        team_with_best_goal_diff = self.stats.get_team_with_best_goal_diff()
        self.assertEqual(team_with_best_goal_diff, 'Manchester United')

    def test_get_top_ten_teams_with_highest_win_pct(self):
        top_ten_teams = self.stats.get_top_ten_teams_with_highest_win_pct()
        self.assertEqual(top_ten_teams[0][0], 'Arsenal')
        self.assertEqual(top_ten_teams[5][0], 'Chelsea')
        self.assertEqual(top_ten_teams[9][0], 'Blackburn')

    def test_get_team_stats_with_most_draws(self):
        team_with_most_draws = self.stats.get_team_stats_with_most_draws()
        self.assertEqual(team_with_most_draws['Team'], 'Derby')
        self.assertEqual(team_with_most_draws['Games'], '38')
        self.assertEqual(team_with_most_draws['Wins'], '8')
        self.assertEqual(team_with_most_draws['Losses'], '6')
        self.assertEqual(team_with_most_draws['Draws'], '24')
        self.assertEqual(team_with_most_draws['Goals'], '33')
        self.assertEqual(team_with_most_draws['Goals Allowed'], '63')
        self.assertEqual(team_with_most_draws['Points'], '30')

    def test_invalid_file(self):
        self.assertRaises(FileNotFoundError, sv.Stats, 'invalid.csv')


if __name__ == '__main__':
    unittest.main()
