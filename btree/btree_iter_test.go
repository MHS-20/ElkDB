package btree

import (
	"fmt"
	"testing"

	is "github.com/stretchr/testify/require"
)

func TestBTreeIter(t *testing.T) {
	{
		c := newC()
		iter := c.tree.SeekLE(nil)
		is.False(t, iter.Valid())
	}

	for _, sz := range []int{5, 2500} {
		c := newC()
		for i := 0; i < sz; i++ {
			c.add(fmt.Sprintf("key%010d", i), fmt.Sprintf("vvv%d", fmix32(uint32(-i))))
		}
		c.verify(t)

		prevk, prevv := []byte(nil), []byte(nil)
		for i := 0; i < sz; i++ {
			key := []byte(fmt.Sprintf("key%010d", i))
			val := []byte(fmt.Sprintf("vvv%d", fmix32(uint32(-i))))

			iter := c.tree.SeekLE(key)
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
