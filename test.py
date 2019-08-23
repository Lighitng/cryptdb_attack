from utils import distance_in_abs, cdf_nearest_search, order_holding, CDF, freq_nearest_search, node_cmp_by_value
import random
from main import Column, Node
if __name__ == "__main__":
  print('----------running test----------')
  for index in range(10):
    col1, col2 = Column('col1'), Column('col2')
    cdf1, cdf2 = 0.0, 0.0
    expect = 0.0
    for idx in range(10):
      temp_node = Node(random.random())
      temp_node.freq = random.random()
      col1.nodes.append(temp_node)
      temp_freq= temp_node.freq
      if idx != 9:
        cdf1 += temp_freq

      temp_node = Node(random.random())
      temp_node.freq = random.random()
      col2.nodes.append(temp_node)
      expect += abs(temp_freq - temp_node.freq)
      if idx != 9:
        cdf2 += temp_node.freq
    result = distance_in_abs(col1, col2)
    print('\tdistance_in_abs(col1, col2), expect: {0}, result: {1}'.format(expect, result) + ('\tpassed' if expect == result else '\tfailed'))
    print('\tCDF(col1.nodes, 9), expect: {0}, result: {1}'.format(cdf1, CDF(col1.nodes, 9)) + ('\tpassed' if cdf1 == CDF(col1.nodes, 9) else '\tfailed'))
