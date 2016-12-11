import firebase as firebase


fb = firebase.FirebaseApplication('https://sentimentcloud.firebaseio.com', None)

print fb.get('/jobs/j-20', None)['results']
