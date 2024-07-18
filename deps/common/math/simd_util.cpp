/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <stdint.h>
#include "common/math/simd_util.h"

#if defined(USE_SIMD)

int mm256_extract_epi32_var_indx(const __m256i vec, const unsigned int i)
{
  __m128i idx = _mm_cvtsi32_si128(i);
  __m256i val = _mm256_permutevar8x32_epi32(vec, _mm256_castsi128_si256(idx));
  return _mm_cvtsi128_si32(_mm256_castsi256_si128(val));
}

int mm256_sum_epi32(const int *values, int size)
{
  __m256i sum = _mm256_setzero_si256();
  int     i;
  for (i = 0; i + SIMD_WIDTH <= size; i += SIMD_WIDTH) {
    __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(values + i));
    sum         = _mm256_add_epi32(sum, vec);
  }

  // 处理最后一个不完整的块
  if (i < size) {
    int     remaining = size - i;
    __m256i mask      = _mm256_set_epi32(remaining > 7 ? -1 : 0,
        remaining > 6 ? -1 : 0,
        remaining > 5 ? -1 : 0,
        remaining > 4 ? -1 : 0,
        remaining > 3 ? -1 : 0,
        remaining > 2 ? -1 : 0,
        remaining > 1 ? -1 : 0,
        remaining > 0 ? -1 : 0);

    int last_block[SIMD_WIDTH] = {0};
    selective_load(const_cast<int *>(values), i, last_block, mask);
    __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(last_block));
    sum         = _mm256_add_epi32(sum, vec);
  }

  int result = 0;
  for (int j = 0; j < SIMD_WIDTH; j++) {
    result += mm256_extract_epi32_var_indx(sum, j);
  }
  return result;
}

float mm256_sum_ps(const float *values, int size)
{
  __m256 sum = _mm256_setzero_ps();
  int    i;
  for (i = 0; i + SIMD_WIDTH <= size; i += SIMD_WIDTH) {
    __m256 vec = _mm256_loadu_ps(values + i);
    sum        = _mm256_add_ps(sum, vec);
  }

  // 处理最后一个不完整的块
  if (i < size) {
    int     remaining = size - i;
    __m256i mask      = _mm256_set_epi32(remaining > 7 ? -1 : 0,
        remaining > 6 ? -1 : 0,
        remaining > 5 ? -1 : 0,
        remaining > 4 ? -1 : 0,
        remaining > 3 ? -1 : 0,
        remaining > 2 ? -1 : 0,
        remaining > 1 ? -1 : 0,
        remaining > 0 ? -1 : 0);

    float last_block[SIMD_WIDTH] = {0.0f};
    selective_load(const_cast<float *>(values), i, last_block, mask);
    __m256 vec = _mm256_loadu_ps(last_block);
    sum        = _mm256_add_ps(sum, vec);
  }

  // 水平求和
  __m128 sum_low  = _mm256_extractf128_ps(sum, 0);
  __m128 sum_high = _mm256_extractf128_ps(sum, 1);
  sum_low         = _mm_add_ps(sum_low, sum_high);
  sum_low         = _mm_hadd_ps(sum_low, sum_low);
  sum_low         = _mm_hadd_ps(sum_low, sum_low);

  return _mm_cvtss_f32(sum_low);
}

template <typename V>
void selective_load(V *memory, int offset, V *vec, __m256i &inv)
{
  int *inv_ptr = reinterpret_cast<int *>(&inv);
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}
template void selective_load<uint32_t>(uint32_t *memory, int offset, uint32_t *vec, __m256i &inv);
template void selective_load<int>(int *memory, int offset, int *vec, __m256i &inv);
template void selective_load<float>(float *memory, int offset, float *vec, __m256i &inv);

#endif