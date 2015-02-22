import os
def get_size(start_path = './data/'):
	
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			if "rel" in f:
				print(f)
				fp = os.path.join(dirpath, f)
				total_size += os.path.getsize(fp)
	return total_size/1024

print(get_size());
