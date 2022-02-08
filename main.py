import os 
from tqdm import tqdm 
import numpy as np
import pandas as pd

# ======================================================================================
#		 								CODE PIPELINE 
# ======================================================================================
# TODO: remove double quote col hack
# TODO: create a sql safe col lookup
# TODO: create a csv lookup from the above transformation 
# ======================================================================================

# find files in dir
def get_files(mode):
	
	# this is going to be our main
	if mode == 0:
		path = 'imports'

		sheets = os.listdir(path)

		return sheets
	# probs avoid using this one 
	elif mode == 1:
		path = input('Please provide a directory to scan: ')

		sheets = os.listdir(path)

		return sheets

# check that all are `.csv`
def clean(sheets):
	
	clean_sheets = []

	# only add those where the file ends in csv 
	for i in sheets:
		# if bool(re.match(r'^.*.csv$', i)):
		if i.endswith('.csv'):
			
			clean_sheets.append(i)

	return clean_sheets

# pull the into mem as a df
def get_sheets(path, mode):

	total_sql = ''
	
	# itterate everything 
	for file in tqdm(path): 
			
		if mode == 0:
			
			exec_path = os.getcwd() + '/imports/'
			dir = exec_path + file

		target_frame = pd.read_csv( dir )
		
		start_types = target_frame.dtypes

		table_sql = 'CREATE TABLE ' + file + ' ( '

		# itterate the end types 
		# TODO: skip the first row and first comma
		for i, row in enumerate(start_types.iteritems()):
			
			# skip first comma
			if i != 0: 
				comma_t = ', '
			else:
				comma_t = ''
				
			# cleaned entries
			column_name = '"' + str(row[0])  + '"'
			sql_d_type = clean_datatype(row[1])
			newline = ' \n'

			# format the block above 
			concat = comma_t + column_name + ' ' +  sql_d_type + newline

			# save outside the loop			
			table_sql = table_sql + concat
		
		total_sql = total_sql + table_sql + ') ; \n\n'

	return total_sql

# convert np.dtype into a sql dtype as string 
def clean_datatype(type):

	if type == np.float64:
		return 'float'
	elif type == np.float32:
		return 'float'
	elif type == np.int32:
		return 'int4'
	elif type == np.int64:
		return 'int8'
	elif type == object:
		return 'text'
	else:
		e = 'Error @ clean_datatype. T:' +  ' unrecognised'
		print( type )
		raise TypeError(e)

# run all of the functions above 
def main(mode): 
			
	sketch_paths = get_files( mode )
	paths = clean( sketch_paths )
	sheets = get_sheets( paths, mode )

	final_sql =  sheets

	with open('finished.sql', 'w') as text_file:
		text_file.write(final_sql)
	
if __name__ == '__main__':
	main(0)