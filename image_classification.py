import tensorflow as tf
import numpy as np
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

class classify:
    def __init__(self):
        self.model = tf.keras.applications.MobileNetV2(weights='imagenet')

    def classify_image(self, image_path):
        img = tf.keras.preprocessing.image.load_img(image_path, target_size=[224, 224])
        img_array = tf.keras.preprocessing.image.img_to_array(img)
        img_array = tf.keras.applications.mobilenet_v2.preprocess_input(img_array[tf.newaxis,...])
        predictions = self.model.predict(img_array)
        classification = tf.keras.applications.mobilenet_v2.decode_predictions(predictions, top=1)[0][0][1]
        return classification
    
def main():
    classifier = classify()
    print(classifier.classify_image('images/d47862af09fc4ed2af7f8cc8894adfd4.jpg'))

if __name__ == '__main__':
    main()