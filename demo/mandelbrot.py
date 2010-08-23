from __future__ import absolute_import

import cmath
import itertools
import logging
import optparse
import os
import sys
import time

log = logging.getLogger("mandelbrot")

def mandelbrot(x, y, depth):
	c = x + y * 1j;
	z = 0
	for i in xrange(depth):
		z = z*z + c
		if abs(z.real) > 2 or abs(z.imag) > 2:
			return i

	return -1

def hsvtorgb(h, s, v):
	i = int(h)
	f = h - i
	if (i & 1 == 0):
		f = 1 - f

	v = v * 255
	m = int(v * (1 - s))
	n = int(v * (1 - s * f))
	v = int(v)

	if i == 0 or i == 6:
		return (v, n, m)
	elif i == 1:
		return (n, v, m)
	elif i == 2:
		return (m, v, n)
	elif i == 3:
		return (m, v, n)
	elif i == 4:
		return (n, m, v)
	elif i == 5:
		return (v, m, n)

class Mandelbrot:
	def __init__(self, width=320, height=180, depth=500, antialias=2, target_x = 0.001643721971153, target_y = 0.822467633298876, zoom = 23):
		self.width = width
		self.height = height
		self.depth = depth
		self.antialias = antialias
		self.target_x = target_x
		self.target_y = target_y
		self.zoom = zoom

		if antialias > 1:
			self.orig_width = self.width
			self.orig_height = self.height
			self.width *= antialias
			self.height *= antialias

		self.coeff = 1.0/self.zoom/min(self.width,self.height)

		self.counter = 0
		self.pixels = self.width * self.height
		self.data = [None] * self.pixels

		self.palette_lookup = [None] * (self.depth + 1)

	def palette(self, i):
		if i == -1:
			return (51, 102, 153)
		i = i % 120
		v = 0.5-float(i)/self.depth*0.2
		return hsvtorgb(i/20.0, v, 1.0)

	def generate_palette(self):
		self.palette_lookup[-1] = self.palette(-1)
		for i in xrange(self.depth):
			self.palette_lookup[i] = self.palette(i)

	def output(self, imagename="mandelbrot.png"):
		log.info("Outputting the image")

		log.info("... generating the palette")
		self.generate_palette()

		log.info("... mapping iteration to the color")
		data = map(lambda x: self.palette_lookup[x], self.data)

		try:
			import PIL.Image as Image

			log.info("... creating image")
			im = Image.new("RGB", (self.width, self.height))

			log.info("... putting data into image")
			im.putdata(data)

			if self.antialias:
				log.info("... stretching down")
				im = im.resize((self.orig_width, self.orig_height), Image.ANTIALIAS)

			log.info("... and finally writing")
			im.save("mandelbrot.png", "PNG")

		except ImportError:
			log.info("... fallbacking to the ppm format")

			if self.antialias:
				log.info("... antialias is not supported for ppm - yet? :(")
				log.info("... use $ convert mandelbrot.ppm -filter Hanning -resize %dx%d mandelbrot.ppm" % (self.orig_width, self.orig_height))

			with open("mandelbrot.ppm", "w") as f:
				f.write("P3\n%d %d\n255\n" % (self.width, self.height));
				for pixel in data:
					f.write("%d %d %d\n" % pixel)

	def batchs(self, n=10000):
		return MandelbrotBatchs(self, n)

	def set_result(self, index, pixels):
		self.data[index:index+len(pixels)] = pixels

class MandelbrotBatchs(object):
	def __init__(self, m, n):
		self.m = m
		self.n = n

	def __iter__(self):
		return self

	def next(self):
		if self.m.counter >= self.m.pixels:
			raise StopIteration

		first = self.m.counter
		last = min(self.m.counter + self.n, self.m.pixels)
		self.m.counter += self.n

		return first, last, self.m.depth, self.m.width, self.m.height, self.m.target_x, self.m.target_y, self.m.coeff

def calculate(index, pixels, depth):
	def calc(e):
		x, y = e
		return mandelbrot(x, y, depth)

	respixels = map(calc, pixels)

	return index, respixels
	# return index, array.array('h', respixels).tostring()

def generate(data):
	first, last, depth, width, height, target_x, target_y, coeff = data

	res = xrange(first, last)

	def maplist(i):
		x = i % width
		y = i // width

		xx = target_x + coeff * (x - width // 2)
		yy = target_y + coeff * (height // 2 - y)
		return (xx, yy)

	return calculate(first, map(maplist, res), depth)

def main():
	from pylvi import Pylvi
	from . import mandelbrot as testmodule

	rootlog = logging.getLogger()
	rootlog.setLevel(logging.INFO)

	handler = logging.StreamHandler()
	formatter = logging.Formatter("%(asctime)s %(levelname)7s %(name)s: %(message)s", "%H:%M:%S")
	handler.setFormatter(formatter)
	rootlog.addHandler(handler)

	parser = optparse.OptionParser()
	parser.add_option("--width", type="int", dest="width", default=384, help="width of the image [%default]")
	parser.add_option("--height", type="int", dest="height", default=216, help="height of the image [%default]")
	parser.add_option("--depth", type="int", dest="depth", default=1000, help="maximum iterations per pixel [%default]")
	parser.add_option("--antialias", type="int", dest="antialias", default=2, help="antialias level, 1 to turn off [%default]")
	parser.add_option("--conf", type="string", dest="conf", help="pylvi cloud configuration")

	(opts,args) = parser.parse_args()

	# Creating mandelbrot
	m = Mandelbrot(width=opts.width, height=opts.height, depth=opts.depth, antialias=opts.antialias)

	with Pylvi(modules=["demo.mandelbrot"]) as pylvi:
		# Adding nodes - always add local - just for sure
		pylvi.add_local_node()

		if opts.conf is not None:
			pylvi.load_conf(opts.conf)

		# few helpers
		# notice that calculation function should be importable (pickable)

		def callback(result):
			index, pixels = result.get()

			# index, packedpixels = result
			# pixels = array.array('h', packedpixels).tolist()

			log.info("job done : %d-%d", index, index+len(pixels)-1)
			m.set_result(index, pixels)

		def tojob(batch):
			return (testmodule.generate, [batch], {}, callback)

		# We can apply many at once
		# Better as we dont need to generate all jobs right away
		pylvi.apply_many(itertools.imap(tojob, m.batchs(n=50000)))

		# or we can add jobs one by one
		#
		#for job in m.batchs(n=20000):
		#	pylvi.apply(testmodule.calculate, args=[job, m.depth], callback=callback)

		# while not done - run loop
		while not pylvi.done():
			log.debug("... jobs not done ...")
			pylvi.run(timeout=1)

	# And we are almost done
	log.info("jobs done!")
	m.output()


if __name__ == "__main__":
	main()
