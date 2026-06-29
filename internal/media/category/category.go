package category

import (
	"strconv"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
)

const defaultRulesYAML = `movie:
  动画片:
    genre_ids: '16'
  纪录片:
    genre_ids: '99'
  儿童家庭:
    genre_ids: '10751'
  动作片:
    genre_ids: '28'
  冒险片:
    genre_ids: '12'
  科幻片:
    genre_ids: '878'
  奇幻片:
    genre_ids: '14'
  悬疑片:
    genre_ids: '9648'
  惊悚片:
    genre_ids: '53'
  恐怖片:
    genre_ids: '27'
  犯罪片:
    genre_ids: '80'
  战争片:
    genre_ids: '10752'
  西部片:
    genre_ids: '37'
  喜剧片:
    genre_ids: '35'
  爱情片:
    genre_ids: '10749'
  剧情片:
    genre_ids: '18'
  历史片:
    genre_ids: '36'
  音乐片:
    genre_ids: '10402'
  电视电影:
    genre_ids: '10770'
  华语电影:
    original_language: 'zh,cn,tw,hk'
  外语电影:
    original_language: '!zh,!cn,!tw,!hk'
tv:
  动漫:
    genre_ids: '16'
  纪录片:
    genre_ids: '99'
  综艺:
    genre_ids: '10764,10767'
  儿童节目:
    genre_ids: '10762'
  国产剧:
    origin_country: 'CN'
    original_language: 'zh,cn'
  港台剧:
    origin_country: 'TW,HK'
  日韩剧:
    origin_country: 'JP,KR'
  欧美剧:
    origin_country: 'US,GB,CA,AU,FR,DE,IT,ES'
  海外其他剧:
    origin_country: '!CN,!TW,!HK,!JP,!KR,!US,!GB'
  未分类:
`

type Metadata struct {
	MediaType        string
	GenreIDs         []int
	OriginCountry    []string
	OriginalLanguage string
}

type Rules struct {
	Sections []Section
}

type Section struct {
	MediaType string
	Rules     []Rule
}

type Rule struct {
	Label            string
	GenreIDs         []string
	OriginCountry    []string
	OriginalLanguage []string
}

func Match(rawYAML string, meta Metadata) string {
	rules, err := Parse(rawYAML)
	if err != nil {
		rules, _ = Parse(defaultRulesYAML)
	}
	if rules == nil {
		return ""
	}
	mediaType := strings.ToLower(strings.TrimSpace(meta.MediaType))
	for _, section := range rules.Sections {
		if strings.ToLower(section.MediaType) != mediaType {
			continue
		}
		for _, rule := range section.Rules {
			if rule.matches(meta) {
				return rule.Label
			}
		}
	}
	return ""
}

func Parse(rawYAML string) (*Rules, error) {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(rawYAML), &root); err != nil {
		return nil, err
	}
	doc := &root
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		doc = root.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return &Rules{}, nil
	}
	rules := &Rules{}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		sectionName := doc.Content[i].Value
		sectionNode := doc.Content[i+1]
		section := Section{MediaType: sectionName}
		if sectionNode.Kind == yaml.MappingNode {
			section.Rules = parseSection(sectionNode)
		}
		rules.Sections = append(rules.Sections, section)
	}
	return rules, nil
}

func parseSection(node *yaml.Node) []Rule {
	var rules []Rule
	for i := 0; i+1 < len(node.Content); i += 2 {
		label := node.Content[i].Value
		rule := Rule{Label: label}
		if node.Content[i+1].Kind == yaml.MappingNode {
			rule = parseRule(label, node.Content[i+1])
		}
		rules = append(rules, rule)
	}
	return rules
}

func parseRule(label string, node *yaml.Node) Rule {
	rule := Rule{Label: label}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.ToLower(strings.TrimSpace(node.Content[i].Value))
		values := splitTokens(node.Content[i+1].Value)
		switch key {
		case "genre_ids":
			rule.GenreIDs = values
		case "origin_country":
			rule.OriginCountry = values
		case "original_language":
			rule.OriginalLanguage = values
		}
	}
	return rule
}

func (r Rule) matches(meta Metadata) bool {
	if len(r.GenreIDs) == 0 && len(r.OriginCountry) == 0 && len(r.OriginalLanguage) == 0 {
		return true
	}
	if len(r.GenreIDs) > 0 && !matchInts(r.GenreIDs, meta.GenreIDs) {
		return false
	}
	if len(r.OriginCountry) > 0 && !matchStrings(r.OriginCountry, meta.OriginCountry) {
		return false
	}
	if len(r.OriginalLanguage) > 0 && !matchStrings(r.OriginalLanguage, []string{meta.OriginalLanguage}) {
		return false
	}
	return true
}

func matchInts(tokens []string, values []int) bool {
	valueSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		valueSet[strconv.Itoa(value)] = struct{}{}
	}
	return matchTokenSet(tokens, valueSet)
}

func matchStrings(tokens []string, values []string) bool {
	valueSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value != "" {
			valueSet[value] = struct{}{}
		}
	}
	return matchTokenSet(tokens, valueSet)
}

func matchTokenSet(tokens []string, values map[string]struct{}) bool {
	hasPositive := false
	positiveMatched := false
	for _, token := range tokens {
		negative := strings.HasPrefix(token, "!")
		token = strings.TrimPrefix(token, "!")
		if _, ok := values[token]; ok {
			if negative {
				return false
			}
			positiveMatched = true
		}
		if !negative {
			hasPositive = true
		}
	}
	if hasPositive {
		return positiveMatched
	}
	return true
}

func splitTokens(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '，' || r == ';' || unicode.IsSpace(r)
	})
	tokens := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.ToUpper(strings.TrimSpace(field))
		if field != "" {
			tokens = append(tokens, field)
		}
	}
	return tokens
}
