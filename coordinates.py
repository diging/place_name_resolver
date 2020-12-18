import sys
import math

def process_length_3_coords(coords):
	d = coords[0]
	m = coords[1]
	se = coords[2]
	dd = 0;
	if d == 0:
		dd = ((m / 60.0) + (se / 3600.0));
	else:
		# python doesn't have a clean way to get the symbol of a number so we use copysign
		dd = math.copysign(1,d) * abs(d) + (m / 60.0) + (se / 3600.0)
	return round(dd, 6)

def process_length_2_coords(coords):
	d = coords[0]
	m = coords[1]
	return round(math.copysign(1,d) * abs(d) + (m / 60.0), 6)

def clean_coordinates(coord):
	split_string =  coord.split("|")
	coords = []
	formatted_coords = []
	for string in split_string:
		try:
			# check if string is a int/float and cast to float
			coords.append(float(string))
		except:
			pass
		if string in ["N", "E"]:
			if len(coords) == 3:
				formatted_coords.append(process_length_3_coords(coords))
				coords.clear()
			if len(coords) == 2:
				formatted_coords.append(clean_coordinates(coord))
				coords.clear()
			if len(coords) == 1:
				formatted_coords.append(coords[0])
				coords.clear()
		elif string in ["S", "W"]:
			# handle negative coords
			if len(coords) == 3:
				formatted_coords.append(process_length_3_coords(coords) * -1)
				coords.clear()
			if len(coords) == 2:
				formatted_coords.append(clean_coordinates(coord) * -1)
				coords.clear()
			if len(coords) == 1:
				formatted_coords.append(coords[0] * -1)
				coords.clear()
	if coords:
		return coords
	return formatted_coords


def main():
	COORD = sys.argv[1]
	test = {'Test': 'Test',
	'cleaned_coordinates': clean_coordinates(COORD)
	}
	print(test)

if __name__ == "__main__":
	main()