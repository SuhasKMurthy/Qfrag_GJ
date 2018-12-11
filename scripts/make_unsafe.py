import sys
import os

filename = sys.argv[1]
file = open(filename, "r")
v_l = {}
new_l = []
for line in file:
	nums = [int(n) for n in line.split()]
	v_l[nums[0]] = nums[1]
	x = (nums[0:2],nums[2:])
	new_l.append(x)
	print(x)

new_fname = os.path.basename(filename)+'.unsafe'
with open(new_fname, 'a') as f:
	for line in new_l:
		label = [v_l[x] for x in line[1]]
		sort_neigh = [x for _,x in sorted(zip(label,line[1]))]
		l = [str(x) for x in line[0]]
		l.extend([str(x) for x in sort_neigh])
		l = ' '.join(l)
		f.write(l+'\n')

