import blockspring

def block(request, response):
	to_return = [[1],[2],[3],[4]]
	change_in_github_ui = 1

	response.addOutput("array", to_return)
	response.end()

blockspring.define(block)
