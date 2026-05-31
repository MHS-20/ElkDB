package btree

import (
	"fmt"
	"testing"

	is "github.com/stretchr/testify/require"
)

func TestBTreeIter(t *testing.T) {
	{
		btt := newBTreeTester()
		iter := btt.tree.SeekLE(nil)
		is.False(t, iter.Valid())
	}

	for _, sz := range []int{5, 2500} {
		btt := newBTreeTester()
		for i := range sz {
			btt.add(fmt.Sprintf("key%010d", i), fmt.Sprintf("vvv%d", fmix32(uint32(-i))))
		}
		btt.verify(t)

		prevk, prevv := []byte(nil), []byte(nil)
		for i := range sz {
			key := fmt.Appendf(nil, "key%010d", i)
			val := fmt.Appendf(nil, "vvv%d", fmix32(uint32(-i)))

			iter := btt.tree.SeekLE(key)
			is.True(t, iter.Valid())
			gotk, gotv := iter.Deref()
			is.Equal(t, key, gotk)
			is.Equal(t, val, gotv)

			iter.Prev()
			if i > 0 {
				is.True(t, iter.Valid())
				gotk, gotv := iter.Deref()
				is.Equal(t, prevk, gotk)
				is.Equal(t, prevv, gotv)
			} else {
				is.False(t, iter.Valid())
			}

			iter.Next()
			gotk, gotv = iter.Deref()
			is.True(t, iter.Valid())
			is.Equal(t, key, gotk)
			is.Equal(t, val, gotv)

			if i+1 == sz {
				iter.Next()
				is.False(t, iter.Valid())
			}

			prevk, prevv = key, val
		}
	}
}
