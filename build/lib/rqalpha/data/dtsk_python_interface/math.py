import numpy as np

def equal(num_left, num_right, precision_in_decimal):
    if abs(num_left - num_right) < np.power(10, -1 * precision_in_decimal):
        return 1
    else:
        return 0

def compare(num_left, num_right, precision_in_decimal):
    if equal(num_left, num_right, precision_in_decimal) == 1:
        return 0
    elif num_left > num_right:
        return 1
    else:
        return -1
