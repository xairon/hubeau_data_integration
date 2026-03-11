"""Timelag matrix generation for SoftCLT — vendorized from github.com/seunghan96/softclt"""

import numpy as np
import torch


def dup_matrix(mat):
    mat0 = torch.tril(mat, diagonal=-1)[:, :-1]
    mat0 += torch.triu(mat, diagonal=1)[:, 1:]
    mat1 = torch.cat([mat0, mat], dim=1)
    mat2 = torch.cat([mat, mat0], dim=1)
    return mat1, mat2


def timelag_sigmoid(T, sigma=1):
    dist = np.arange(T)
    dist = np.abs(dist - dist[:, np.newaxis])
    matrix = 2 / (1 + np.exp(dist * sigma))
    matrix = np.where(matrix < 1e-6, 0, matrix)
    return matrix


def timelag_gaussian(T, sigma):
    dist = np.arange(T)
    dist = np.abs(dist - dist[:, np.newaxis])
    matrix = np.exp(-(dist**2) / (2 * sigma**2))
    matrix = np.where(matrix < 1e-6, 0, matrix)
    return matrix
