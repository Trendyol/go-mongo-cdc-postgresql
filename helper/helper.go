package helper

// ChunkSliceWithSize splits the given slice into chunks with a maximum size of `size`.
func ChunkSliceWithSize[T any](slice []T, size int) [][]T {
	if size <= 0 {
		return [][]T{slice}
	}

	chunks := make([][]T, 0, (len(slice)+size-1)/size)
	for size < len(slice) {
		slice, chunks = slice[size:], append(chunks, slice[0:size:size])
	}
	if len(slice) > 0 {
		chunks = append(chunks, slice)
	}

	return chunks
}
