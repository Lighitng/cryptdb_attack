def distance_in_abs(col1, col2):
  # diffierent number of node will cause extra distance
  freq_dist = 0.0
  extra_dist = abs(len(col1.nodes) - len(col2.nodes)) * 1.0 / len(col1.nodes)
  for (index, node) in enumerate(col1.nodes):
    if index >= len(col2.nodes):
      break
    freq_dist += abs(node.freq - col2.nodes[index].freq)
  return freq_dist + extra_dist

def CDF(nodes, pos):
  if pos <= 0:
    return 0
  else:
    cdf = 0.0
    for index in range(pos):
      cdf += nodes[index].freq
    return cdf

def order_holding(index, matched_flag):
  for idx in range(index + 1, len(matched_flag)):
    if matched_flag[idx] != 0:
      return False
  return True

def cdf_nearest_search(ary, value, matched_flag):
  nearest = 0
  while matched_flag[nearest] != 0:
    nearest += 1
  for (index, el) in enumerate(ary):
    if abs(value - el) < abs(value - ary[nearest]) and matched_flag[index] == 0 and order_holding(index, matched_flag):
      nearest = index
  return nearest

def freq_nearest_search(nodes, value, matched_flag):
  nearest = 0
  while matched_flag[nearest] != 0:
    nearest += 1
  for (index, node) in enumerate(nodes):
    if abs(value - node.freq) < abs(value - nodes[nearest].freq) and matched_flag[index] == 0:
      nearest = index
  return nearest

def node_cmp_by_value(node1, node2):
  if node1.value < node2.value:
    return -1
  elif node1.value > node2.value:
    return 1
  else:
    return 0
