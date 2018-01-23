import commands

def tone():
	user=raw_input('This is a Tone Analyzer. Please enter the text you want:')
	print commands.getoutput("curl -XGET --header \'Content-Type:application/json\' -d \'{\"text\":\""+user+"\"}\' \'127.0.0.1:5000\'")

def text():
	t = raw_input('Please enter the file you want to listen:')
	print commands.getoutput("curl -XPOST -F \'file=@"+t+"\' -o \'result.wav\' \'127.0.0.1:5000/doc\'")
	print 'The audio is saved in result.wav'


def word():
	print 'Do you want to type a text or use a text file?'
	print '1: I\'ll type it'
	print '2: I\'ll use a file'

	ch = raw_input()
	if ch == '1':
		user=raw_input('Please enter the text:')
		print commands.getoutput("curl -XGET --header \'Content-Type:application/json\' -d \'{\"text\":\"" +user+ "\"}\' \'127.0.0.1:5000/word\'")
	else:
		user=raw_input('Please enter the file:')
		print commands.getoutput("curl -XPOST -F \'file=@"+user+"\' \'127.0.0.1:5000/word\'")

