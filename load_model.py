import os
import numpy as np
from tensorflow import keras
import tensorflow as tf
import imageio
# new_model = tf.keras.models.load_model('resnet_model')
# sample_image = keras.preprocessing.image.load_img(
#     '/public/mp4-other-version/cat_dog_testing_set/cats/cat.4023.jpg',
#     target_size=(224, 224)
# )
# sample_image = np.array(sample_image)
# sample_image = np.expand_dims(sample_image, axis=0)
# sample_image = keras.applications.resnet50.preprocess_input(sample_image)
# print(new_model.predict(sample_image)[0][0] < 0.5)

mnist_model = tf.keras.models.load_model('lenet_model')

def reshape_input(image_path:str):
    img = imageio.imread(image_path).astype(np.float)
    pad_image = tf.pad(img, [[2,2], [2,2]])/255
    ret = tf.expand_dims(pad_image, axis=0, name=None)
    ret = tf.expand_dims(ret, axis=3, name=None)
    return ret

prediction = mnist_model.predict(reshape_input("image_test/71.png"))
print("predicted digit:", str(prediction))
classes_x=np.argmax(prediction,axis=1)
print(classes_x[0])