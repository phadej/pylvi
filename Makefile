.PHONY : demo

readme:
	@pod2readme README.pod - | less

demo:
	time python -m demo.mandelbrot

profile:
	python -m cProfile -o mandelbrot.prof mandelbrot.py
	python -m pstats mandelbrot.prof

clean:
	rm -f *.pyc mandelbrot.prof
