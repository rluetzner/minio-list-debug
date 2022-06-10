set datafile separator ';'
set xlabel "Num. files"
set ylabel "Time / s"

set ytics nomirror

# Add a grid
set style line 100 lt 1 lc rgb "grey" lw 0.5
set grid ls 100

# Rotate x values
# set xtics rotate
# Don't show every single value on x
set xtics 500

f(x) = m * x + q
fit[0:15000] f(x) filename using 1:2 via m,q

# mq_value = sprintf("Parameters values\nm = %f k$/m^2\nq = %f k$", m, q)
# set object 1 rect from 90,725 to 200, 650 fc rgb "white"
# set label 1 at 100,700 mq_value

plot filename using 1:2 with linespoint, f(x) ls 2 t 'Linear regression (range 0-15,000)'
