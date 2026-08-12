package cluster

import "strings"

func Mobile139MaxSingleUploadBytes(tier string) int64 {
	return mobile139MaxSingleUploadBytes(tier)
}

func MembershipWeight(tier string) int {
	return membershipWeight(tier)
}

func mobile139MaxSingleUploadBytes(tier string) int64 {
	switch normalizeMembershipTier(tier) {
	case "diamond":
		return 500 << 30
	case "gold":
		return 20 << 30
	case "silver":
		return 8 << 30
	case "ordinary", "normal":
		return 5 << 30
	default:
		return 0
	}
}

func membershipWeight(tier string) int {
	switch normalizeMembershipTier(tier) {
	case "diamond":
		return 400
	case "gold":
		return 300
	case "silver":
		return 200
	case "ordinary", "normal":
		return 100
	default:
		return 0
	}
}

func normalizeMembershipTier(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "diamond", "钻石":
		return "diamond"
	case "gold", "黄金":
		return "gold"
	case "silver", "白银":
		return "silver"
	case "ordinary", "normal", "普通", "普通会员", "非会员", "free":
		return "ordinary"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}
