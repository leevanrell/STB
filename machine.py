#!/usr/bin/python3

from sklearn.model_selection import train_test_split
import tensorflow as tf
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from lib.machine.DF import DF

DAYS = 14
Stock_file = './data/stock.db'
Wiki_file = './data/wiki.db'


def main():
	df = DF(Stock_file, Wiki_file)
	RNN_df = df.data.drop(['EMA20', 'SMA20', 'SMA50', 'SMA200', 'RSI', 'MACD', 'views', 'one', 'volume'], axis=1)
	RNN_df = RNN_df.dropna()
	#print(RNN_df)
	RNN(RNN_df)


def feedforward(df):
	#https://www.kaggle.com/hbaderts/simple-feed-forward-neural-network-with-tensorflow/notebook
	X_train = df.drop('one', axis=1).as_matrix()
	y_train = df['one'].as_matrix()

	X_train, X_test, y_train, y_test = train_test_split(X_train, y_train, test_size=0.2)

	# three classifiers: buy, hold, sell
	labels_train = (np.arange(2) == y_train[:,None]).astype(np.float32)
	labels_test = (np.arange(2) == y_test[:,None]).astype(np.float32)

	inputs = tf.placeholder(tf.float32, shape=(None, X_train.shape[1]), name='inputs')
	label = tf.placeholder(tf.float32, shape=(None, 3), name='labels')

	# First layer
	hid1_size = 128
	w1 = tf.Variable(tf.random_normal([hid1_size, X_train.shape[1]], stddev=0.01), name='w1')
	b1 = tf.Variable(tf.constant(0.1, shape=(hid1_size, 1)), name='b1')
	y1 = tf.nn.dropout(tf.nn.relu(tf.add(tf.matmul(w1, tf.transpose(inputs)), b1)), keep_prob=0.5)

	# Second layer
	hid2_size = 256
	w2 = tf.Variable(tf.random_normal([hid2_size, hid1_size], stddev=0.01), name='w2')
	b2 = tf.Variable(tf.constant(0.1, shape=(hid2_size, 1)), name='b2')
	y2 = tf.nn.dropout(tf.nn.relu(tf.add(tf.matmul(w2, y1), b2)), keep_prob=0.5)

	# Output layer
	wo = tf.Variable(tf.random_normal([2, hid2_size], stddev=0.01), name='wo')
	bo = tf.Variable(tf.random_normal([2, 1]), name='bo')
	yo = tf.transpose(tf.add(tf.matmul(wo, y2), bo))

	# Loss function and optimizer
	lr = tf.placeholder(tf.float32, shape=(), name='learning_rate')
	loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=yo, labels=label))
	optimizer = tf.train.GradientDescentOptimizer(lr).minimize(loss)

	# Prediction
	pred = tf.nn.softmax(yo)
	pred_label = tf.argmax(pred, 1)
	correct_prediction = tf.equal(tf.argmax(pred, 1), tf.argmax(label, 1))
	accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))

	# Create operation which will initialize all variables
	init = tf.global_variables_initializer()

	# Configure GPU not to use all memory
	config = tf.ConfigProto()
	config.gpu_options.allow_growth = True

	# Start a new tensorflow session and initialize variables
	sess = tf.InteractiveSession(config=config)
	sess.run(init)

	# This is the main training loop: we train for 50 epochs with a learning rate of 0.05 and another 
	# 50 epochs with a smaller learning rate of 0.01
	for learning_rate in [0.05, 0.01]:
		for epoch in range(50):
			avg_cost = 0.0

			# For each epoch, we go through all the samples we have.
			for i in range(X_train.shape[0]):
				# Finally, this is where the magic happens: run our optimizer, feed the current example into X and the current target into Y
				_, c = sess.run([optimizer, loss], feed_dict={lr:learning_rate, inputs: X_train[i, None], label: labels_train[i, None]})
				avg_cost += c
			avg_cost /= X_train.shape[0]

			# Print the cost in this epcho to the console.
			if epoch % 10 == 0:
				print("Epoch: {:3d}    Train Cost: {:.4f}".format(epoch, avg_cost))

	acc_train = accuracy.eval(feed_dict={inputs: X_train, label: labels_train})
	print("Train accuracy: {:3.2f}%".format(acc_train*100.0))

	acc_test = accuracy.eval(feed_dict={inputs: X_test, label: labels_test})
	print("Test accuracy:  {:3.2f}%".format(acc_test*100.0))


def RNN(df):
	#https://mapr.com/blog/deep-learning-tensorflow/
	TS = np.array(df)
	periods = 600
	f_horizon = 1

	x_data = TS[:(len(TS) - (len(TS) % periods))]
	x_batches = x_data.reshape(-1, 600, 1)

	y_data = TS[1:(len(TS) - (len(TS) % periods)) + f_horizon]
	y_batches = y_data.reshape(-1, 600, 1)

	test_x_setup = TS[-(periods + f_horizon):]
	X_test = test_x_setup[:periods].reshape(-1, 600, 1)
	Y_test = TS[-(periods):].reshape(-1, 600, 1)

	tf.reset_default_graph()

	inputs = 1
	hidden = 100
	output = 1

	X = tf.placeholder(tf.float32, [None, periods, inputs])
	y = tf.placeholder(tf.float32, [None, periods, output])

	basic_cell = tf.contrib.rnn.BasicRNNCell(num_units=hidden, activation=tf.nn.relu)
	rnn_output, states = tf.nn.dynamic_rnn(basic_cell, X, dtype=tf.float32)

	learning_rate = 0.001

	stacked_rnn_output = tf.reshape(rnn_output, [-1, hidden])
	stacked_outputs = tf.layers.dense(stacked_rnn_output, output)
	outputs = tf.reshape(stacked_outputs, [-1, periods, output])

	loss = tf.reduce_sum(tf.square(outputs - y))
	optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
	training_op = optimizer.minimize(loss)

	init = tf.global_variables_initializer()

	epochs = 5000

	with tf.Session() as sess:
		init.run()
		for ep in range(epochs):
			sess.run(training_op, feed_dict={X: x_batches, y: y_batches})
			if ep % 100 == 0:
				mse = loss.eval(feed_dict={X: x_batches, y: y_batches})
				print(ep, "\t MSE:", mse)
		y_pred = sess.run(outputs, feed_dict={X: X_test})
		print(y_pred)

	plt.title('Forecast vs. Actual', fontsize=14)
	plt.plot(pd.Series(np.ravel(Y_test)), 'bo', markersize=10, label='Actual')
	plt.plot(pd.Series(np.ravel(y_pred)), 'r.', markersize=10, label='Forecast')
	plt.legend(loc='upper left')
	plt.xlabel('Time periods')
	plt.show()


if __name__ == '__main__':
	main()
