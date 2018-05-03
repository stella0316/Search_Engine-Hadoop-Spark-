import subprocess

def prompt():
	while  True:
		search_type = input("Select type(s) of search you want to do:\n 1 = Title, 2= Column, 3= Content, 4 = Topic\n Separate by comma if you select multiple types:\n" )


		values = search_type.split(',')
		wrong=False

		for i in values:
			if i not in ['1','2','3']:
				print('Input can only be integers: 1,2,3 separated by comma')
				wrong = True
			if wrong == True:
				continue
                
		if wrong == False:
			break
    
           
	keywords = input("Enter keywords for your search separated by comma:\n")

	while len(keywords) == 0 or keywords == ' ':
		print('Please enter valid input')
		keywords = input("Enter keywords for your search separated by comma")

	words = keywords.split(',')

    	#Filter
	row_filter = input("Please enter minimum number of rows per table. Enter n to ignore:\n")

	while len(row_filter) != 1:
		print('Please enter valid input')
		row_filter = input("Please enter minimum number of rows per table. Enter n to ignore:\n")

	if row_filter == 'n' or 'N':
		pass
	else:
		while True:
			try:
				row_filter = int(row_filter)
				break
			except ValueError:
				print('Please enter valid input')
				row_filter = input("Please enter minimum number of rows per table. Enter n to ignore")
                
	return search_type, words, row_filter

def main():
	search_type, words, row_filter = prompt()
	process = subprocess.check_output(["bash","start_program.sh",search_type,words,row_filter])

if __name__ == "__main__":
	main()


