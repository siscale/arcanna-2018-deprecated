def validate_request(current_app,data, context):
	# Todo: take out temp token var 
	context += ["token","apiToken"]
	result = True 
        # To revert back after test 
        for k in context:
                if k not in data.keys():
                        result = False 
        if "token" in data.keys() or "apiToken" in data.keys():
		result = True
	if result is False:
		return False 

	if "token" in data.keys():
		token = data['token']
	elif "apiToken" in data.keys():
		token = data['apiToken'] 

        if token is None or token != current_app.config["API_TOKEN"]:
		return False
	return True 

def validate_action(requested_action, valid_actions):
	if requested_action not in valid_actions.keys():
		return False
	return True
