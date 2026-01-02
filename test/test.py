from src.dispatcher import DEFER
import threading
from tensorflow.keras.applications.resnet50 import ResNet50, preprocess_input, decode_predictions
from tensorflow.keras.preprocessing import image
import numpy as np
import queue
import time

defer = DEFER()

model = ResNet50(weights='imagenet', include_top=True)
model.summary()
# Depending on the number of compute nodes, use different number of partitions
# "part_at" is where the graph is split, so there should be one less element in this
# list than the number of partitions you want
part_at = [
    "conv2_block1_add",
    "conv2_block3_add",
    "conv3_block2_add",
    "conv3_block4_add",
    "conv4_block2_add",
    "conv4_block4_add",
    "conv5_block1_add"
]
img_path = '../resource/test.jpg'
img = image.load_img(img_path, target_size=(224, 224))
x = image.img_to_array(img)
x = np.expand_dims(x, axis=0)
x = preprocess_input(x)

start = time.time()
test_size = 2
def print_result(q):
    res_count = 0
    start_time = time.time()
    while test_size > res_count:
        res = q.get()
        res_count += 1
        print(res.shape)
    end_time = time.time()
    run = end_time - start_time
    time_min = run / 60
    print(f"{res_count} results in {time_min} min")
    print(f"Throughput: {res_count / run} req/s")
    exit(0)

input_q = queue.Queue(10)
output_q = queue.Queue(10)

a = threading.Thread(target=defer.run_defer, args=(model, part_at, input_q, output_q), daemon=True)
b = threading.Thread(target=print_result, args=(output_q,))
a.start()
b.start()

for i in range(test_size):
    # Whatever input you want
    input_q.put(x)

b.join()