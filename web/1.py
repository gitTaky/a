from a import *

while 1:
	print 'What are you want to do?'
	print '1: Tone Analyzer'
	print '2: Text to Speech'
	print '3: Word Count'

	user = raw_input()

	if user == '1':
		tone()
	elif user == '2':
		text()
	else:
		word()


