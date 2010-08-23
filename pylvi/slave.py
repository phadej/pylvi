import errno
import fcntl
import logging
import multiprocessing
import os
import pickle
import select
import signal
import struct
import sys
import tempfile
import traceback

log = logging.getLogger(os.uname()[1])
log.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("         %(levelname)7s %(name)s: %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)

run = True
modulepath = None
cpucount = 1
try:
	cpucount = multiprocessing.cpu_count()
except NotImplementedError:
	pass

class Processor:
	def __init__(self, cpucount):
		self.jobs = {}
		self.pool = multiprocessing.Pool(cpucount)

	def process_job(self, job_id, job):
		func, args, kwargs = job
		log.info("job received %d", job_id)

		self.jobs[job_id] = self.pool.apply_async(func, args, kwargs)

	def __iter__(self):
		return self

	def complete(self):
		self.pool.close()
		self.pool.join()

	def next(self):
		log.debug("processor next, jobs %d", len(self.jobs))

		for job_id in self.jobs.keys():
			#log.debug("checking job %d", job_id)

			if self.jobs[job_id].ready():
				try:
					res = self.jobs[job_id].get()
					del self.jobs[job_id]
					return (True, job_id, res)
				except Exception as e:
					return (False, job_id, e)

		#log.debug("no ready jobs")
		raise StopIteration

processor = None

class IO:
	def __init__(self):
		self.outfd = sys.stdout.fileno()
		self.infd = sys.stdin.fileno()
		self.readbuffer = ""
		self.writebuffer = ""

		options = fcntl.fcntl(self.outfd, fcntl.F_GETFL)
		fcntl.fcntl(self.outfd, fcntl.F_SETFL, options | os.O_NONBLOCK)

	def write(self, buf):
		self.writebuffer += buf
		self.push()

	def push(self, timeout=1):
		if self.writebuffer == "":
			return

		log.debug("pushing")

		# (r,w,x) = select.select([], [self.outfd], [], timeout)

		#log.debug("writing")
		try:
			written = os.write(self.outfd, self.writebuffer)
			#log.debug("written")

			self.writebuffer = self.writebuffer[written:]
		except OSError as e:
			if e.errno == errno.EAGAIN:
				pass
			else:
				raise e

	def write_message(self, msg):
		self.enqueue_message(msg)
		self.push()

	def enqueue_message(self, msg):
		log.info("enqueuing message(%s)", msg["type"])
		m = pickle.dumps(msg)
		p = struct.pack("I", len(m))

		self.writebuffer += p + m

	def read(self, size, timeout=None):
		if len(self.readbuffer) >= size:
			buf = self.readbuffer[0:size]
			self.readbuffer = self.readbuffer[size:]
			return buf

		(r,w,x) = select.select([self.infd], [], [], timeout)
		if r == []:
			return None

		tmp = os.read(self.infd, 65536)
		if tmp == "":
			raise IOError, "end-of-file"

		self.readbuffer = self.readbuffer + tmp

		if len(self.readbuffer) > size:
			buf = self.readbuffer[0:size]
			self.readbuffer = self.readbuffer[size:]
			return buf

		return None

	def read_blocking(self, size):
		while len(self.readbuffer) < size:
			(r,w,x) = select.select([self.infd], [], [])

			tmp = os.read(self.infd, 65536)
			if tmp == "":
				raise IOError, "end-of-file"

			self.readbuffer = self.readbuffer + tmp

		buf = self.readbuffer[0:size]
		self.readbuffer = self.readbuffer[size:]
		return buf

	def read_message(self, timeout=1):
		header = self.read(4, timeout=timeout)
		if header == None:
			return None

		size, = struct.unpack("I", header)
		msg = self.read_blocking(size)

		return pickle.loads(msg)

	def __iter__(self):
		return self

	def next(self):
		msg = self.read_message()
		if msg is None:
			raise StopIteration

		return msg

io = IO()

def process_message(msg):
	global run
	global modulepath
	global processor

	t = msg["type"]

	if t == "ping":
		return {'type': 'pong', 'i': msg['i']}

	elif t == "shutdown":
		log.info("shutdown message received")
		if processor is not None:
			processor.complete()

		run = False

	elif t == "import":
		log.info("import message received")

		modulefd, modulepath = tempfile.mkstemp(".zip")
		log.debug("adding to the modulepath %s", modulepath)

		os.write(modulefd, msg['data'])
		os.close(modulefd)

		sys.path.insert(0, modulepath)

	elif t == "job":
		# Asynchronous way
		if processor is None:
			processor = Processor(cpucount)

		processor.process_job(msg['jobid'], msg['job'])

	else:
		log.warn("unknown message type %s", t)
		return {'type': 'duh'}

def terminate(signum, frame):
	log.info("signal catched %d", signum)
	run = False

signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

for i in xrange(cpucount + cpucount // 2 + 1):
	io.write_message({'type': 'newjob'})

while run:
	try:
		log.debug("slave loop enters, reading messages")

#		max_messages = 0
#		for msg in io:
#			reply = process_message(msg)
#			if reply is not None:
#				io.enqueue_message(reply)
#
#			max_messages += 1
#			if max_messages >= 3:
#				break
		msg = io.read_message()
		if msg is not None:
			reply = process_message(msg)
			if reply is not None:
				io.enqueue_message(reply)

		log.debug("checking completed jobs")

		if processor is not None:
			for successful, job_id, result in processor:
				if successful:
					log.info("job done %d", job_id)
					io.enqueue_message({'type': 'jobdone', 'jobid': job_id, 'result': result})
				else:
					log.error("job throw an excpetion %d", job_id)
					io.enqueue_message({'type': 'jobfailed', 'jobid': job_id, 'e': result})

		io.push()

	except Exception as exc:
		log.error("exception '%s' catched: %s", type(exc).__name__, str(exc))
		traceback.print_exc()
		io.write_message({"type": "exception", "e": exc})
		break

# cleanup

if modulepath is not None:
	log.debug("removing modulezip %s", modulepath)
	os.unlink(modulepath)
else:
	log.error("modulepath is None")

log.info("exiting")
