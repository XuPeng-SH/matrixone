package io

//func TestStaticFilterIndex(t *testing.T) {
//	bufManager := buffer.NewNodeManager(1024*1024, nil)
//	file := dataio.MockIndexFile()
//	var err error
//	var res bool
//	var exist bool
//	var ans *roaring.Bitmap
//	var meta *common.IndexMeta
//	cType := common.Plain
//	typ := types.Type{Oid: types.T_int32}
//	colIdx := uint16(0)
//
//	writer := NewStaticFilterIndexWriter()
//	err = writer.Init(file, cType, colIdx)
//	require.NoError(t, err)
//
//	keys := common.MockVec(typ, 1000, 0)
//	err = writer.AddValues(keys)
//	require.NoError(t, err)
//
//	meta, err = writer.Finalize()
//	require.NoError(t, err)
//
//	reader := NewStaticFilterIndexReader()
//	err = reader.Init(bufManager, file, meta)
//	require.NoError(t, err)
//
//	//t.Log(bufManager.String())
//
//	res, err = reader.MayContainsKey(int32(500))
//	require.NoError(t, err)
//	require.True(t, res)
//
//	res, err = reader.MayContainsKey(int32(2000))
//	require.NoError(t, err)
//	require.False(t, res)
//
//	query := common.MockVec(typ, 1000, 1500)
//	exist, ans, err = reader.MayContainsAnyKeys(query, nil)
//	require.NoError(t, err)
//	require.True(t, ans.GetCardinality() < uint64(10))
//	require.True(t, exist)
//
//	//t.Log(bufManager.String())
//}
