"""
SoftCLT losses — vendorized from github.com/seunghan96/softclt

Provides `hierarchical_contrastive_loss` as a drop-in replacement for TS2Vec's loss.
The soft version uses sigmoid-based timelag weighting for temporal CL
and distance-based soft labels for instance CL.
"""

import torch
import torch.nn.functional as F
import numpy as np
from .timelags import dup_matrix, timelag_sigmoid
from .hard_losses import inst_CL_hard, temp_CL_hard


def inst_CL_soft(z1, z2, soft_labels_L, soft_labels_R):
    B, T = z1.size(0), z1.size(1)
    if B == 1:
        return z1.new_tensor(0.)
    z = torch.cat([z1, z2], dim=0)
    z = z.transpose(0, 1)
    sim = torch.matmul(z, z.transpose(1, 2))
    logits = torch.tril(sim, diagonal=-1)[:, :, :-1]
    logits += torch.triu(sim, diagonal=1)[:, :, 1:]
    logits = -F.log_softmax(logits, dim=-1)
    i = torch.arange(B, device=z1.device)
    loss = torch.sum(logits[:, i] * soft_labels_L)
    loss += torch.sum(logits[:, B + i] * soft_labels_R)
    loss /= (2 * B * T)
    return loss


def temp_CL_soft(z1, z2, timelag_L, timelag_R):
    B, T = z1.size(0), z1.size(1)
    if T == 1:
        return z1.new_tensor(0.)
    z = torch.cat([z1, z2], dim=1)
    sim = torch.matmul(z, z.transpose(1, 2))
    logits = torch.tril(sim, diagonal=-1)[:, :, :-1]
    logits += torch.triu(sim, diagonal=1)[:, :, 1:]
    logits = -F.log_softmax(logits, dim=-1)
    t = torch.arange(T, device=z1.device)
    loss = torch.sum(logits[:, t] * timelag_L)
    loss += torch.sum(logits[:, T + t] * timelag_R)
    loss /= (2 * B * T)
    return loss


def _compute_soft_labels(z1):
    """Compute instance-wise soft labels from representation similarity."""
    B = z1.size(0)
    # Mean pool over time dimension to get instance representations
    z_mean = z1.mean(dim=1)  # B x C
    # Cosine similarity matrix
    z_norm = F.normalize(z_mean, dim=1)
    sim_matrix = torch.matmul(z_norm, z_norm.T)  # B x B
    # Convert to soft labels via softmax
    soft_labels = F.softmax(sim_matrix, dim=1)
    return soft_labels


def hierarchical_contrastive_loss(z1, z2, alpha=0.5, temporal_unit=0):
    """
    Drop-in replacement for TS2Vec's hierarchical_contrastive_loss.

    Uses soft temporal assignments (sigmoid timelag) and soft instance assignments.
    Same signature as TS2Vec's loss for monkey-patch compatibility.
    """
    tau_temp = 2.0
    lambda_ = alpha
    soft_temporal = True
    soft_instance = True

    # Compute instance-wise soft labels
    soft_labels = _compute_soft_labels(z1)
    soft_labels_L, soft_labels_R = dup_matrix(soft_labels)

    loss = torch.tensor(0., device=z1.device)
    d = 0
    while z1.size(1) > 1:
        if lambda_ != 0:
            if soft_instance:
                loss += lambda_ * inst_CL_soft(z1, z2, soft_labels_L, soft_labels_R)
            else:
                loss += lambda_ * inst_CL_hard(z1, z2)
        if d >= temporal_unit:
            if 1 - lambda_ != 0:
                if soft_temporal:
                    timelag = timelag_sigmoid(z1.shape[1], tau_temp * (2**d))
                    timelag = torch.tensor(timelag, device=z1.device, dtype=torch.float32)
                    timelag_L, timelag_R = dup_matrix(timelag)
                    loss += (1 - lambda_) * temp_CL_soft(z1, z2, timelag_L, timelag_R)
                else:
                    loss += (1 - lambda_) * temp_CL_hard(z1, z2)
        d += 1
        z1 = F.max_pool1d(z1.transpose(1, 2), kernel_size=2).transpose(1, 2)
        z2 = F.max_pool1d(z2.transpose(1, 2), kernel_size=2).transpose(1, 2)

    if z1.size(1) == 1:
        if lambda_ != 0:
            if soft_instance:
                loss += lambda_ * inst_CL_soft(z1, z2, soft_labels_L, soft_labels_R)
            else:
                loss += lambda_ * inst_CL_hard(z1, z2)
        d += 1

    return loss / d
