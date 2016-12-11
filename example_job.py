import firebase as firebase


fb = firebase.FirebaseApplication('https://sentimentcloud.firebaseio.com', None)

job = {
    'duration': 30,
    'status': 'waiting',
    'keywords': 'happy'
}

fb.put('/jobs', 'j-20', job)