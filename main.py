from init_table import createTable
import MySQLdb
import re
import csv
import copy
from config import database_config

db = MySQLdb.connect(host=database_config['host'], port=3306, user=database_config['user'], passwd=database_config['password'], db=database_config['schema'], charset='utf8')
cursor = db.cursor()
lossy_constant = 0.1
lossy = 0.1
density_standard = 0.8
matched_flag = list()

# each value in enum will construct an 'Node' obj
class Node:
  def __init__(self, value):
    self.counter = 1
    self.value = value
    self.match = None
  def __lt__(self, other):
    if self.counter < other.counter:
      return True
    else:
      return False
  def add_count(self):
    self.counter += 1
# column obj
class Column:
  # init
  # @param {str} col_name
  def __init__(self, col_name):
    self.col_name = col_name
    self.counter = 0
    self.freq = 0
    self.real_col_name = None
    self.nodes = list()
  # @param {value} str/int
  def find_node_value(self, value):
    for node in self.nodes:
      if node.value == value:
        return node
    return None
  def handle_dataset(self, data):
    total = float(len(data))
    if total == 0:
      return None
    for row in data:
      node = self.find_node_value(row[0])
      if node:
        node.add_count()
      else:
        self.nodes.append(Node(row[0]))
    self.nodes.sort()
    for node in self.nodes:
      node.freq = node.counter / total
    # for node in self.nodes:
    #   print('value: {0} counter: {1}'.format(node.value, node.counter))
  def data_decrypt(self, dataset):
    decrypt_set = list()
    for data in dataset:
      for node in self.nodes:
        if node.value == data[0]:
          decrypt_set.append(node.match)
    return decrypt_set

def insert_data (db, cursor):
  # insert_template = 'insert into {0}'.format(database_config['table']) + '(`team_abbr`,`team_div`,`play_stat`,`play_pos`, `play_height`, `play_weight`, `play_PF`)' + ' value ("{0}","{1}","{2}","{3}",{4},"{5}",{6})'
  csv_reader = csv.reader(open("./data/2016.csv"))
  assist_cols = [Column(col_name) for col_name in database_config['columns']]
  assist_data = [[] for _ in database_config['columns']]
  for (index, row) in enumerate(csv_reader):
    if index <= 0:
      continue
    # cursor.execute(insert_template.format(*row))
    for (index, data) in enumerate(assist_data):
      data.append((row[index],))
    # let cryptdb undo the first layer
  # db.commit()
  for (index, col) in enumerate(assist_cols):
    col.handle_dataset(assist_data[index])
  return assist_cols

def distance_in_abs(col1, col2):
  # diffierent number of node will cause extra distance
  freq_dist = 0.0
  extra_dist = abs(len(col1.nodes) - len(col2.nodes)) * 1.0 / len(col1.nodes)
  for (index, node) in enumerate(col1.nodes):
    if index >= len(col2.nodes):
      break
    freq_dist += abs(node.freq - col2.nodes[index].freq)
  return freq_dist + extra_dist


def Match_columns(assist_cols, encrypted_cols):
  global lossy
  global lossy_constant
  match_cols = copy.deepcopy(assist_cols)
  matched_col_names = []
  for (index, col) in enumerate(encrypted_cols):
    alternative_cols = []
    match_cols[index] = None
    while match_cols[index] is None:
      for assist_col in assist_cols:
        if len(assist_col.nodes) >= (1 - lossy) * len(col.nodes) and len(assist_col.nodes) <= (1 + lossy) * len(col.nodes):
          alternative_cols.append(assist_col)
      alter_index = -1
      for (idx, alter) in enumerate(alternative_cols):
        try:
          matched_col_names.index(alter.col_name)
          continue
        except:
          alter_index = idx
          break
      if len(alternative_cols) == 0 or alter_index == -1:
        lossy += 0.1
        print('No match column for {0}. lossy is increased'.format(col.col_name))
      else:
        for idx in range(alter_index, len(alternative_cols) - 1):
          if distance_in_abs(col, alternative_cols[alter_index]) > distance_in_abs(col, alternative_cols[idx]):
            alter_index = idx
        match_cols[index] = alternative_cols[alter_index]
        col.real_col_name = match_cols[index].col_name
        matched_col_names.append(match_cols[index].col_name)
    lossy = lossy_constant
  # for debug
  # for index in range(7):
  #   print('----------col {}----------'.format(str(index)))
  #   print('\tencrypted_col_node_len: ' + str(len(encrypted_cols[index].nodes)))
  #   if match_cols[index] is not None:
  #     print('\tassist_col_node_len: ' + str(len(match_cols[index].nodes)))
  #   else:
  #     print('\tassist_col is none !!!')
  return [encrypted_cols, match_cols]

def CDF(nodes, pos):
  if pos <= 0:
    return 0
  else:
    cdf = 0.0
    for index in range(pos):
      cdf += nodes[index].freq
    return cdf

def order_holding(index):
  global matched_flag
  for idx in range(index + 1, len(matched_flag)):
    if matched_flag[idx] != 0:
      return False
  return True

def cdf_nearest_search(ary, value):
  global matched_flag
  nearest = 0
  while matched_flag[nearest] != 0:
    nearest += 1
  for (index, el) in enumerate(ary):
    if abs(value - el) < abs(value - ary[nearest]) and matched_flag[index] == 0 and order_holding(index):
      nearest = index
  return nearest

def freq_nearest_search(nodes, value):
  global matched_flag
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

def DET_attack(assist_cols, encrypted_cols):
  global matched_flag
  encrypted_set, assist_set = Match_columns(assist_cols, encrypted_cols)
  for (index, e_col) in enumerate(encrypted_set):
    a_col = assist_set[index]
    matched_flag = [0 for _ in range(len(a_col.nodes))]
    if len(e_col.nodes) == len(a_col.nodes):
      for (idx, node) in enumerate(e_col.nodes):
        node.match = a_col.nodes[idx].value
    elif len(e_col.nodes) > len(a_col.nodes):
      print('assist data is not enough')
      exit(1)
    else:
      for (idx, node) in enumerate(e_col.nodes):
        freq_pos = freq_nearest_search(a_col.nodes, node.freq)
        matched_flag[freq_pos] = 1
        node.match = a_col.nodes[freq_pos].value
  return encrypted_set

def OPE_attack(assist_cols, encrypted_cols):
  global matched_flag
  assist_cdf = list()
  encrypted_set, assist_set = Match_columns(assist_cols, encrypted_cols)
  # resort by value
  for (index, e_col) in enumerate(encrypted_set):
    e_col.nodes.sort(cmp=node_cmp_by_value)
    assist_set[index].nodes.sort(cmp=node_cmp_by_value)
  #construct cdf
  for assist_col in assist_cols:
    assist_cdf.append([CDF(assist_col.nodes, index) for index in range(len(assist_col.nodes))])
  for (index, e_col) in enumerate(encrypted_set):
    a_col = assist_set[index]
    a_cdf = assist_cdf[index]
    matched_flag = [0 for _ in range(len(a_col.nodes))]
    if len(e_col.nodes) == len(a_col.nodes):
      for (idx, node) in enumerate(e_col.nodes):
        node.match = a_col.nodes[idx].value
    elif len(e_col.nodes) > len(a_col.nodes):
      print('assist data is not enough')
      exit(1)
    else:
      for (idx, node) in enumerate(e_col.nodes):
        if idx == 0:
          node.match = a_col.nodes[0].value
          matched_flag[0] = 1
        node_cdf = CDF(e_col.nodes, idx)
        cdf_pos = cdf_nearest_search(a_cdf, node_cdf)
        # avoid for mismatch
        if cdf_pos - idx >= len(a_col.nodes) - len(e_col.nodes):
          for i in range(idx, len(e_col.nodes)):
            e_col.nodes[i].match = a_col.nodes[len(a_col.nodes) - len(e_col.nodes) + i].value
          break
        matched_flag[cdf_pos] = 1
        node.match = a_col.nodes[cdf_pos].value
  return encrypted_set
      
def decrypt_and_output(matched_cols, columns, data, output_filename):
  # title
  title = list()
  ordered_matched_cols = list()
  for col_name in columns:
    for m_col in matched_cols:
      if col_name == m_col.col_name:
        title.append(m_col.real_col_name)
        ordered_matched_cols.append(m_col)
  # construct rows
  data_rows = list()
  decrypted_cols = list()
  for (index, dataset) in enumerate(data):
    decrypted_cols.append(ordered_matched_cols[index].data_decrypt(dataset))
  for index in range(len(decrypted_cols[0])):
    data_rows.append([col[index] for col in decrypted_cols])
  # for row in data_rows:
  #   print(row)
  # write result
  out = open('./data/{0}.csv'.format(output_filename), 'w')
  writer = csv.writer(out, dialect='excel')
  writer.writerow(columns)
  writer.writerow(title)
  writer.writerows(data_rows)
    # print('\tlen: '+str(len(col.nodes)))
  # then frequency will contain all the columns in the form of dict
  for col in matched_cols:
    print('----------col {0} : {1} match result----------'.format(col.col_name, col.real_col_name))
    for node in col.nodes:
      print('\tencrypted: {0}\tdecrypted: {1}'.format(node.value, node.match))

if __name__ == '__main__':
  createTable(cursor)
  print('--------collecting data---------')
  assist_cols = insert_data(db, cursor)
  print('finished\n--------decrypting data--------')
  cursor.execute('show columns from crypted')
  columns_detail = cursor.fetchall()
  columns = list()
  encrypted_det_cols = list()
  encrypted_ope_cols = list()

  # read DET tables
  for col in columns_detail:
    if re.match('cdb_salt', col[0]) or re.search('DET$', col[0]) is None:
      continue
    columns.append(col[0])
  query_template = 'select {0} from crypted'
  DET_crypted_data = list()
  for col_name in columns:
    # print(query_template.format(col_name))
    cursor.execute(query_template.format(col_name))
    # construct a dict
    data = cursor.fetchall()
    DET_crypted_data.append(data)
    col = Column(col_name)
    col.handle_dataset(data)
    encrypted_det_cols.append(col)
  # for (index, col) in enumerate(assist_cols):
    # print('-------------assit_col {0}-------------'.format(str(index)))
    # for node in col.nodes:
    #   print('\tfreq: {0}'.format(node.freq))
    # print('\tlen: '+str(len(col.nodes)))
  # for (index, col) in enumerate(encrypted_det_cols):
    # print('-------------encrypted_col {0}-------------'.format(str(index)))
    # for node in col.nodes:
    #   print('\tfreq: {0}'.format(node.freq))
  matched_cols = DET_attack(assist_cols, encrypted_det_cols)
  decrypt_and_output(matched_cols, columns, DET_crypted_data, 'DET_decrypt_2017')

  # --------------------------ope attack--------------------------------
  print('----------OPE ATTACK----------')
  columns = list()
  for col in columns_detail:
    if re.match('cdb_salt', col[0]) or re.search('OPE$', col[0]) is None:
      continue
    columns.append(col[0])
  query_template = 'select {0} from crypted'
  OPE_crypted_data = list()
  for col_name in columns:
    # print(query_template.format(col_name))
    cursor.execute(query_template.format(col_name))
    # construct a dict
    data = cursor.fetchall()
    OPE_crypted_data.append(data)
    col = Column(col_name)
    col.handle_dataset(data)
    encrypted_ope_cols.append(col)
  # for (index, col) in enumerate(assist_cols):
  #   print('-------------assit_col {0}-------------'.format(str(index)))
  #   for node in col.nodes:
  #     print('\tfreq: {0}'.format(node.freq))
    # print('\tlen: '+str(len(col.nodes)))
  # for (index, col) in enumerate(encrypted_ope_cols):
  #   print('-------------encrypted_col {0}-------------'.format(str(index)))
  #   for node in col.nodes:
  #     print('\tfreq: {0}'.format(node.freq))
  matched_cols = OPE_attack(assist_cols, encrypted_ope_cols)
  decrypt_and_output(matched_cols, columns, OPE_crypted_data, 'OPE_decrypt_2017')
  
  

