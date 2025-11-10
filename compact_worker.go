package cmap

// import (
// 	"log"
// 	"time"
// )

// // Example of a smart compaction loop
// func bucketCompactionWorker(b *bucket) {
// 	ticker := time.NewTicker(10 * time.Second)
// 	defer ticker.Stop()

// 	var stats Stats
// 	const compactionThreshold = 0.3 // e.g., compact if 30% of space is dead

// 	for range ticker.C {
// 		b.UpdateStats(&stats)

// 		totalBytes := stats.LiveBytes + stats.DeadBytes
// 		if totalBytes == 0 {
// 			continue
// 		}

// 		fragmentationRatio := float64(stats.DeadBytes) / float64(totalBytes)

// 		if fragmentationRatio > compactionThreshold {
// 			log.Printf("high fragmentation detected (%.2f%%), compacting bucket...", fragmentationRatio*100)
// 			if err := b.Compact(); err != nil {
// 				log.Printf("error during compaction: %v", err)
// 			}
// 		}
// 	}
// }
