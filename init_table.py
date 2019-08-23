# init
from config import database_config

def createTable (cursor):
  cursor.execute('drop table if exists {0}'.format(database_config['table']))
  cursor.execute('''
    CREATE TABLE `{0}` (
      `team_id` int(11) NOT NULL AUTO_INCREMENT,
      `team_abbr` char(20) NOT NULL,
      `team_div` char(20) NOT NULL,
      `play_stat` char(20) NOT NULL,
      `play_pos` char(20) NOT NULL,
      `play_height` int NOT NULL,
      `play_weight` char(20) NOT NULL,
      `play_PF` int DEFAULT NULL,
      PRIMARY KEY (`team_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
  '''.format(database_config['table']))
# return ['`team_abbr`', '`team_div`', '`play_stat`', '`play_pos`', '`play_height`', '`play_weight`', '`play_PF`']

